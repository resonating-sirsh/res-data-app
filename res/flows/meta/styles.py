import res
import pandas as pd
import json
from res.flows.FlowContext import FlowContext
from res.media.images import outlines
import hashlib
from res.media.images.providers import DxfFile, pdf
from ast import literal_eval
from res.connectors.s3 import datalake
from res.connectors.airtable import AirtableConnector
from .marker import MetaMarker

# bodies = list_astms(None)
# all_colors = list_body_colors(body=None)
# digital_assets = pd.merge(all_colors, bodies, on=['body_code'], suffixes=['_bc', '_meta'])

STYLE_FILE = "s3://res-data-platform/samples/data/styles_for_validation.csv"


def validations():
    """
    if all the tests below pass, we can generate the one spec for the style
    if we generate a one spec for the file an order can be mapped to a one spec -> pieces are projected and tracked in make

    some of these are true row ops but some need slower processing e.g. actualy naming pieces means we need to apply the ASTM to the raster image which is slow
    however this is like a final step in create a one-spec; if we success we create the one spec as the gate and if we fail we publish the failure to kafka

    some of these can only be determined from left joins e.g. if we have a style size but no digital asset for it - a size mapping error would be of this kind
    this one could also be just a case of SPEC_MISSING which means that for one of the other reasons we did not have correct assets to generate the spec

    """

    def MAPPED_META_PIECE_NAMES(row):
        """
        all piece names on the styles existing in the meta body
        if there is no mapping e.g. for mono material we can trust the pieces "exist" but that are separately validated against the ASTM
        this really looks for invalid mappings when they exist
        """

        piece_names = row.get("alt_piece_names")
        meta_piece_names = row["dxf_metadata_res_piece_names"]

        # ignore??
        if not isinstance(piece_names, list):
            return True

        intersection = set(piece_names) & set(meta_piece_names)
        return len(intersection) == len(piece_names)

    def VALID_META_PIECE_NAMES(row):
        """
        the piece names conform to new piece naming logic
        """

    def VALID_META_VERSION(row):
        """
        the meta version is not behind the color version
        """
        try:
            # format
            v = row["meta-version"]
            return v < row["body_version"]
        except:
            return True

    def VALID_COLOR_VERSION(row):
        """
        the color version is not behind the meta version
        """
        try:
            # format from the path-only part of the raster
            v = int(row["path_bc"].split("/")[-5].replace("v", ""))
            return v >= row["body_version"]
        except:
            return True

    def COLOR_PIECE_VALID(row):
        """
        material cuttable width, missing pixels
        """

    def MAPPED_SIZE(row):
        """
        All sizes in the style that are required, have digital and meta assets
        """

    def NAMED_PIECES_WITHIN_CONFIDENCE(row):
        """
        The meta piece geometry adheres to and can name the color piece within confidence
        """

    v = {
        "MAPPED_META_PIECE_NAMES": MAPPED_META_PIECE_NAMES,
        "VALID_META_PIECE_NAMES": VALID_META_PIECE_NAMES,
        "VALID_META_VERSION": VALID_META_VERSION,
        "VALID_COLOR_VERSION": VALID_COLOR_VERSION,
    }

    return v


@res.flows.flow_node_attributes(
    memory="4Gi",
)
def reducer(event, context):
    """
    Update the marker stats
    This could be done in a number of places but typically we will want some sort of management process to trigger marker updates for some asset requests
    when we do this, piggy  back here to update all stats

    res_data_res_data_marker_stats

    """
    from res.flows.meta.marker import migrate_marker_stats

    res.utils.logger.info("Migrating stats for all recently updated markers")

    migrate_marker_stats()

    return {}


@res.flows.flow_node_attributes(
    memory="1Gi",
)
def generator(event, context={}):
    """
    read from a source of styles TBD - for testing a saved CSV
    par over the styles and process them in parallel
    """
    s3 = res.connectors.load("s3")

    def event_with_assets(a):
        e = {}
        for k in ["metadata", "apiVersion", "kind", "args", "task"]:
            e[k] = event[k]

        a["key"] = a.get("key", a.get("res_key"))
        # TODO - this is not necessarily they key we want but its ok for now

        e["task"] = {
            "key": (a["key"] + "-" + str(a["version"])).replace(" ", "-").lower()
        }

        if not isinstance(a, list):
            a = [a]
        e["assets"] = a

        return e

    assets = [
        event_with_assets(row)
        for row in event["assets"]
        # s3.read(STYLE_FILE).to_dict("records")
    ]

    return assets


VALIDATIONS = ["MAPPED_META_PIECE_NAMES", "VALID_META_VERSION", "VALID_COLOR_VERSION"]


def augment_and_validate(data):
    data["dxf_metadata"] = data["path_meta"].map(datalake.dxf_metadata)
    data = res.utils.dataframes.expand_column(data, "dxf_metadata").drop(
        ["dxf_metadata", "dxf_metadata_piece_codes"], 1
    )
    data["primary_material"] = data["res_key"].map(lambda x: x.split(" ")[1])

    ops = validations()

    for k in VALIDATIONS:
        data[k] = data.apply(ops[k], axis=1)

    return data


@res.flows.flow_node_attributes(
    memory="50Gi",
)
def handler(event, context={}):
    """
    event = {
        "metadata": {"name": "dxa.generate_style", "node": "name_pieces_export_styles"},
        "assets" : assets,
        "task": {
        "key": "X"
        },
        "args":{  }
    }

    """
    with FlowContext(event, context) as fc:
        result = fc.apply(
            "export_styles",
            export_styles,
            fc.assets,
            key=fc.key,
            # because we are mapping to a more advanced contract if required
            resolve_asset_contract=True,
            use_cache=True,
            # pass the flow context
            fc=fc,
        )
        return {}


def handler_old(event, context):
    """
    read the styles file and process one asset


    """

    def try_literal_eval(s):
        try:
            return literal_eval(s)
        except:
            return None

    with res.flows.FlowContext(event, context) as fc:
        # load the entire file and filter by the asset keys
        # ths is a pattern that maps over a file in a lazy way without sending all the files row data
        data = fc.connectors["s3"].read(STYLE_FILE)
        keys = fc.assets_dataframe["key"].values
        data = data[data["res_key"].isin(keys)].reset_index(drop=True)

        for col in [
            "alt_piece_mapping",
            # we could cache these but also can just generate below
            # "dxf_metadata_sizes",
            # "dxf_metadata_res_piece_names",
            "material_properties",
        ]:
            data[col] = data[col].map(try_literal_eval)

        try:
            data = augment_and_validate(data)
        except Exception as ex:
            res.utils.logger.warn(f"FAIL: cannot do validation {repr(ex)}")
            return data
        # for each style that we process save the outout data that can be compiled later

        key = data.iloc[0]["res_key"]
        df = fc.apply(
            "export_style_gate",
            export_styles,
            data,
            key=key.replace(" ", "-"),
        )

        # this is like returning a sample or if we process row at a time returns all the data
        return df


def make_one_spec_key(row, m=5):
    """
    <body_code>-V<style_body_version>-<size>-<color>-<alt_piece_hash>
    """
    style_pieces = {
        "primary": row["primary_material"],
        "pieces": row["alt_piece_mapping"],
    }

    h = json.dumps(style_pieces, sort_keys=True).encode("utf-8")

    h = hashlib.shake_256(h).hexdigest(m).upper()

    return f"{row['body_code']}-V{int(row['body_version'])}-{row['size']}-{row['color_code']}-{h}"


def _extract_images_from_path(path):
    """
    images can be stored in different ways
    two cases we handle are just loose in folder and other is zip pdfs
    we could also have zipped pngs and other things
    """
    s3 = res.connectors.load("s3")

    if path[-4:] == ".zip":
        for name, image in pdf.iterate_s3_pdf_archive_images(path):
            channels = image.shape[-1]
            res.utils.logger.debug(
                f"Extracting {name} from zip {path} - image has {channels} channels"
            )
            if channels < 4:
                res.utils.logger.debug(
                    f"*********SKIPIING 3 CHANNEL EXTRACTED FROM PDF FOR NOW*********"
                )
                continue
            yield name, image
    else:
        for file in s3.ls(path):
            yield file, s3.read(file)


def export_styles(
    assets, resolve_asset_contract=False, fc=None, use_cache=False, **kwargs
):
    """

    This is the WIP flow for mapping styles to Meta Markers
    There is a lot we can do around validation and automation beyond what we have done in the past
    The meta marker is a really good place to coordinate these things

    assets: sized style assets, each of which is a style that can be exploded into pieces using information about materials, colors and meta bodies
    the contract has everything of interest on a style and resolved digital assets that have passed tests as assets on a meta level prior to trying to process
    contract:
      key should be the res sku and size for now
      we should generate the one-spec key as a function of whats on the contract
      we need all the style extended props for print type and materials
      we need the digital assets and some of their metadata

    Buffered pieces dont match astm - but for block fuse you can try adding 0.75 * 300 as a block fuse buffer to name pieces on some styles
      for other pieces we require that the recorded buffer offset matche what was used to create the color on shape

    EXAMPLE style request with fully resolved properties
    Note for a color on shape we look for the latest matching astm but we could force to try newer meta

        {'res_key': 'TK6077 CHRST ARTNKD',
        'body_version': 7.0,
        'body_code': 'TK-6077',
        'secondary_material': nan,
        'print_type': 'Placement',
        'alt_piece_mapping': nan,
        'alt_piece_names': nan,
        'color_code': 'ARTNKD',
        'material_properties': "{'CHRST': {'compensation_length': 1.0126582278481011, 'compensation_width': 1.1111111111111112, 'cuttable_width': 57.0, 'material_stability': 'Stable', 'offset_size': nan}}",
        'primary_material': 'CHRST',
        'contains_unstable_materials': False,
        'path_meta': 's3://meta-one-assets-prod/bodies/tk_6077/pattern_files/body_tk_6077_v6_pattern.dxf',
        'path_bc': 's3://meta-one-assets-prod/color_on_shape/tk_6077/v6/artnkd/3zzmd/rasterized_files',
        'size': '3ZZMD',
        'normed_size': 'M',
        'bc_version': 'v6',
        'meta_version': 'body_tk_6077_v6_pattern.dxf'}

    From styles we can resolve
    #1 optionally force a version instead of latest e.g. if we dont have assets on latest version
    s =  dict(read_styles('TK6077 BCICT LEMOCT', force_version=6).iloc[0])
    #2 we can augment the assets for all styles sizes - optionally uses the latest meta data e.g. a body on v6 with a n astm on v7
    style_request = gen_style_assets_for_sizes(s, sizes =['3ZZMD'], allow_newer_meta=True)
    # 3 this creates the asset payload we need to export the style as a meta marker
    export_styles(style_request,
       #unless buffers are deprecated we need to apply the block fuse buffer approx to name pieces
    block_fuse_buffer=0.75 * 300,
       #we can try to generate directional color on shape if a human doesnt make one
    allow_missing_directional_color_files=True)

    """

    s3 = res.connectors.load("s3")

    def validate_piece_image(row):
        """
        validate against material width and also check the image statistics for things like missing pixels
        we can important this from an image library maybe if the row proviodes an interface for physical stretching etc.
        """

        def f(p):
            return None

        return f

    results = []

    trust_archived_image_file_names = kwargs.get("is_trusting_archive_filenames", False)

    if isinstance(assets, dict):
        # assume list intended
        assets = [assets]

    for asset in assets:
        res_key = asset["key"]
        res.utils.logger.info(f"Input asset {asset}")
        req_size = asset.get("size")
        if resolve_asset_contract:
            v = asset.get("version")
            allow_newer_meta = asset.get("allow_newer_meta", True)

            res.utils.logger.info(
                f"""resolving full style details and generating asset request for meta marker and one-spec:
                 style key {asset['key']}. Requested version is {v} and allow newer meta is {allow_newer_meta}"""
            )
            # this mode generates just the style request asset row
            # we will fid a more efficient way to do this in future e.g. caching or transactional database

            if use_cache:
                # try the cache first
                res.utils.logger.info(
                    f'Reading the style {asset["key"]} from the cache'
                )
                asset = read_style_cache(asset["key"])
                if not asset:
                    res.utils.logger.info(f"not in cache...")
                    asset = dict(read(asset["key"], force_version=v).iloc[0])
                else:
                    res.utils.logger.info(f"found in cache: {asset}")
            else:
                asset = dict(read(asset["key"], force_version=v).iloc[0])

            asset = dict(
                gen_style_assets_for_sizes(
                    asset, sizes=[req_size], allow_newer_meta=allow_newer_meta
                ).iloc[0]
            )

            _path_bc = asset.get("path_bc")
            if pd.isnull(_path_bc):
                res.utils.logger.warn(
                    f"unable to resolve a color on shape file for this style: {str(asset)}"
                )
                if fc:
                    fc.publish_asset_status(
                        asset, "create_meta_marker", "NO_BODY_COLOR", "FAILED"
                    )
                continue
            _path_meta = asset.get("path_meta")
            if pd.isnull(_path_meta):
                res.utils.logger.warn(
                    f"unable to resolve a meta body file for this style: {str(asset)}"
                )
                fc.publish_asset_status(
                    asset, "create_meta_marker", "NO_BODY_META", "FAILED"
                )
                continue

        one_spec_key = make_one_spec_key(asset)
        asset["key"] = one_spec_key

        ##NOTE WE ARE CHOOSING TO TRY BUFFER FUNC
        dxf = s3.read(asset["path_meta"], fuse_buffer_function=True)
        # if single raster we can deal with it but assume multiple
        # deprecate use of specific rasterized files in path. contract requires folder of rasters
        # raster = asset["path_bc"].split("/")[-1]
        path = asset["path_bc"]  # .replace(f"{raster}", "")
        if not kwargs.get("allow_missing_directional_color_files"):
            assert (
                path is not None
            ), "There is no valid path (location) for the color on shape files for this asset"
        res.utils.logger.info(
            f"*******  Attempt to make {one_spec_key} using meta {asset['path_meta']} and raster path {path}  *******"
        )

        # TODO sizes should always be strings on the way in and on the way out
        gerber_size = dxf.map_res_size_to_gerber_size(str(asset["normed_size"]))

        material_props = asset["material_properties"]

        # test - this should be determine from the material that the piece would be in and then lookup the material stability chart
        piece_material_buffers = (
            {}
        )  # {k:300 if k in ['KT-6005-V7-FT_LG','KT-6005-V7-BK_LG'] else 900  for k, v in named_pieces.items()}

        def piece_material_offset(p):
            mat = asset["alt_piece_mapping"]
            if pd.isnull(mat):
                mat = {}
            mat = mat.get(p, asset["primary_material"])
            offset = material_props[mat].get("offset_size", None)
            if pd.isnull(offset):
                return None
            return offset * 300

        # dxf_piece_names = dxf._get_res_piece_names()
        # ^ if we need to coerce somehow ??
        dxf_piece_names = dxf.primary_piece_codes
        res.utils.logger.info(f"DXF piece names are {dxf_piece_names}")
        if material_props:
            piece_material_buffers = {
                k: piece_material_offset(k) for k in dxf_piece_names
            }
            # remove void cases
            piece_material_buffers = {
                k: v for k, v in piece_material_buffers.items() if pd.notnull(v) and v
            }

        # res.utils.logger.info(piece_material_buffers)
        bf_buffer = kwargs.get("block_fuse_buffer")
        res.utils.logger.info(
            f"Naming pieces with gerber size {gerber_size} with block fused buffer: {bf_buffer} and piece material buffers {piece_material_buffers}"
        )

        named_pieces = None
        try:
            named_pieces = dxf.get_named_piece_image_outlines(
                size=gerber_size,
                piece_types=["self", "block_fuse"],
                # you can try a block fuse buffer for some styles: 0.75 * 300
                block_fuse_buffer=bf_buffer,
                material_props=piece_material_buffers,
            )
        except Exception as ex:
            # raise ex
            res.utils.logger.warn(
                f"Failed to extract data from the DXF file when getting named pieces outlines: {repr(ex)}"
            )
            if fc:
                fc.publish_asset_status(
                    asset, "create_meta_marker", "DXF_PARSE", "FAILED"
                )
            continue

        res.utils.logger.info(
            f"There are {len(named_pieces)} self or block fuse pieces in the gerber size {gerber_size}"
        )

        mapping = asset.get("alt_piece_mapping", None)
        piece_materials = {
            "default": asset["primary_material"],
        }
        if pd.notnull(mapping):
            piece_materials["pieces"] = mapping

        vfn = validate_piece_image(asset)
        piece_validation_errors = {}
        confidences_out = {}

        # constant list of names we have added to pre validate styles - useful to add to the output
        style_val = {k: asset.get(k) for k in VALIDATIONS}

        # TODO refactor this block and also think about piece level contract
        for image_filename, image in _extract_images_from_path(path):
            # these are two support paths but we need to be careful about contract in future
            piece_path = path.replace("rasterized_files/", "named_pieces/")
            piece_path = piece_path.replace("zipped_pieces.zip", "named_pieces/")
            # temp waiting for convention
            if ".zip" in piece_path:
                piece_path = f"{s3.object_dir(piece_path)}named_pieces/"

            # start path validations
            assert (
                "named_pieces" in piece_path
            ), f"the piece path {piece_path} is invalid"

            # this should only be for testing but we can pluck vAlt pieces and save them in the body version that we are creating
            body_version = f"/v{asset['body_version']}"
            meta_version = f"/v{asset['meta-version']}"
            # we can override this for special cases
            piece_path = piece_path.replace(body_version, meta_version)

            # we dont need to continue looking at rasters if all pieces named
            if len(named_pieces) == 0:
                continue

            res.utils.logger.info(
                f"Process file {image_filename} and saving pieces in {piece_path}. Body version={body_version} and Meta version={meta_version} - pieces to name : {len(named_pieces)}"
            )

            # if we have an option to trust the file names then here we just save the file and add a result entry and continue to the next piece
            if trust_archived_image_file_names:
                piece_image = f"{piece_path}{image_filename}.png"
                piece_validation_errors[name] = vfn(part)
                if not piece_validation_errors[name]:
                    res.utils.logger.debug(
                        f"saving file {piece_image} - we decided to trust the file names for piece matching to DXF file"
                    )
                    results.append(
                        {
                            "raster": image_filename,
                            "name": image_filename,
                            "confidence": 1,
                        }
                    )
                    s3.write(piece_image, part)
                continue

            for name, part in outlines.enumerate_pieces(
                image,
                named_pieces,
                # this may depend on the flow
                invert_named_pieces=False,
                log_file_fn=fc.get_logging_function(
                    "mask", key=res.utils.hash_of(image_filename)
                )
                if fc
                else None,
                piece_material_buffers=piece_material_buffers,
                confidences_out=confidences_out,
            ):
                piece_image = f"{piece_path}{name}.png"
                res.utils.logger.debug(f"saving file {piece_image}")
                piece_validation_errors[name] = vfn(part)
                if not piece_validation_errors[name]:
                    s3.write(piece_image, part)

                # for piece and style header validations data
                results.append(
                    {
                        "raster": image_filename,
                        "name": name,
                        "confidence": confidences_out.get(name),
                        "one_spec_key": one_spec_key,
                        **style_val,
                    }
                )

        # create in the flow context a piece confidence log and we can presto it

        # after processing all rasterized files we should used up all our names
        if len(named_pieces):  # or other validations such as piece validations
            # publish asset fail / feedback queue ?
            res.utils.logger.warn(
                f"We were not able to name all the pieces for {one_spec_key} in confidence: {list(named_pieces.keys())}"
            )
            if fc:
                fc.publish_asset_status(
                    asset, "create_meta_marker", "PIECE_NAMES", "FAILED"
                )
            continue

        marker_path = DxfFile.export_meta_marker(
            dxf,
            sizes={gerber_size: asset["size"]},
            color_code=asset["color_code"],
            piece_material_map=piece_materials,
            key=one_spec_key,
            material_props=material_props,
            # todo add material props
            # todo try fetch sew instructions
            metadata={
                "meta_path": asset["path_meta"],
                "path_bc": asset["path_bc"],
                "res_key": res_key,
                "meta_sample_size": dxf.sample_size,
            },
        )

        res.utils.logger.info("exporting nesting statistics for marker")
        marker = MetaMarker(marker_path)

        # this is also a good gate on reloading the meta marker so leave it
        nstat = marker.get_nesting_statistics()
        s3.write(f"{marker_path}/nesting_stats.feather", nstat)

        # this is also a good gate on reloading the meta marker so leave it
        rec_id = asset.get("record_id")
        costs = get_style_cost_table(rec_id, asset["size"])
        if pd.notnull(rec_id) and costs is not None:
            res.utils.logger.info("exporting style costs for marker")
            s3.write(f"{marker_path}/cost_table.feather", costs)
        else:
            res.utils.logger.warn(
                f"unable to export style costs for marker where asset key is {asset['key']}"
            )

        if fc:
            # if we have a marker we can make it
            asset["value"] = marker_path
            fc.publish_asset_status(asset, "create_meta_marker", "", "DONE")

        # we can save this somewhere useful - dynamo for now
        marker.cache_summary()

        res.utils.logger.info("Style exported.")

    return pd.DataFrame(results)


def extract_and_validate_pieces():
    pass


def _get_meta(body, version, use_latest=False):
    """
    load the meta path
    """
    files = datalake.list_astms(body, drop_dups=False).sort_values("meta_version")
    if use_latest:
        return files.iloc[-1]["path"]
    else:
        df = files[files["meta_version"] == version]
        if len(df):
            return df.iloc[-1]["path"]
    return None


def _get_bc(body, version, color_code, size):
    """
    load the body color file path
    """
    df = datalake.list_body_colors(body)
    # df['color_meta_version'] = df['color_meta_version'].astype(int)
    df = df[df["color_meta_version"] == version]
    if len(df) == 0:
        return None

    df = df[(df["color_code"] == color_code.upper()) & (df["size"] == size.upper())]
    if len(df) == 0:
        return None
    return df.iloc[-1]["path"]


def gen_3d_style_bundle_assets_for_sizes(style, sizes=None, body_version=None):
    """
    utility to link styles to paths on s3 - this is expected to evolve and be refactored

     "dxf_path": "s3://meta-one-assets-prod/color_on_shape/tt_3036/v1/citrbq/dxf/TT-3036-V1-3D_BODY-2022-02-17T14:20:39.628Z.dxf",
     "pieces_path": "s3://meta-one-assets-prod/color_on_shape/tt_3036/v1/citrbq/1zzxs/pieces",


    body_meta = dict(style_request[["size", "path_meta"]].values)
    body_color = dict(style_request[["size", "path_bc"]].values)

    """
    s3 = res.connectors.load("s3")

    body_lower = style["body_code"].replace("-", "_").lower()
    color = style["color_code"]
    version = f"v{style['body_version']}"
    if body_version:
        res.utils.logger.info(
            f"Using the requested body version {body_version} in place of the style latest"
        )
        version = f"v{body_version}"

    #
    root = f"s3://meta-one-assets-prod/color_on_shape/{body_lower}/{version}/{color}".lower()
    body_root = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_lower}/{version}/extracted"

    # if there are none or more than more thats a problem
    pt = f"{body_root}/dxf"
    res.utils.logger.info(pt)

    pts = list(s3.ls(pt))

    pts = [f for f in pts if ".dxf" in f.lower()]
    if len(pts) == 0:
        raise Exception(f"The path {pt} does not contain any dxf files")

    dxf_file = pts[0]

    data = []
    for s, gs in style["size_map"].items():
        pt = f"{root}/{s}/pieces".lower()
        data.append({"size": s, "path_meta": dxf_file, "path_bc": pt})

    return pd.DataFrame(data)


def gen_style_assets_for_sizes(style, sizes=None, allow_newer_meta=False):
    """
    lookup the data lake for assets with some versioning rules
    return the assets and any possible version drift
    it is not valid to look up a bc off version but the meta version might be newer for testing
    in future we might support some older versions but unlikely as this is a gate
    """
    body_code = style["body_code"]
    color_code = style["color_code"]
    size_map = style["size_map"]
    version = style["body_version"]

    meta = _get_meta(body_code, version, use_latest=allow_newer_meta)
    meta_version = lambda r: int(
        r.lower().split("/")[-1].split("_")[-2].replace("v", "")
    )
    all_styles = []

    def make_object(size, nsize, bc, meta):
        s = {}
        s.update(dict(style))
        s["size"] = size
        s["normed_size"] = nsize
        s["path_meta"] = meta
        s["path_bc"] = bc
        s["meta-version"] = meta_version(meta)

        return s

    for size, nsize in size_map.items():
        if sizes is not None and size not in sizes:
            print(size, "missing when generating styles assets for size")
            continue
        bc = _get_bc(body_code, version, color_code, size=size)
        all_styles.append(make_object(size, nsize, bc, meta))

    return pd.DataFrame(all_styles).drop(["sizes", "size_map"], 1)


def read_style_cache(code=None):
    """
    If we trust some style cache - which we do for testing -
    we can avoid the airtable lookup and try to load it from there

    Example:
        read_style_cache('CC9009 CTW70 MOONPK')

    """
    from ast import literal_eval
    import json

    def f(s):
        if isinstance(s, str):
            s = s.replace("nan", "None")
            try:
                return literal_eval(s)
            except:
                try:
                    return json.loads(s)
                except:
                    return None
        else:
            return s

    df = pd.read_csv("s3://res-data-development/cache/styles.csv")
    df = df.where(pd.notnull(df), None)

    for c in ["normed_size", "material_properties", "size_map"]:
        df[c] = df[c].map(f)

    if code:
        try:
            return dict(df.set_index("res_key").loc[code])
        except:
            return None
    return df


# @retry(wait=wait_fixed(3), stop=stop_after_attempt(4))
def read(
    styles=None,
    return_source_styles=False,
    force_version=None,
    allow_non_one_ready=True,
    style_id=None,
):
    """
    temporary - we will maintain this contract in dgraph when settled

    we add on digital assets here as a gate for styles unless return styles true

    this is nothing more than a join on digital assets and styles with cleaned up contract
    the color on shape digital asset is just a path and the real data is in dgraph. this function can be used to put the data in dgraph though so we
    just need to describe where the source data is
    in the case of the astm, this should already be ingested and we can use the DXF or dgraph to load data

    the contract can be send to style ingestion as an asset in the job

    contract:

      ['key', 'body_version', 'body_code', 'piece_mapping', 'secondary_material', 'print_type', 'alt_piece_names',
       'color_code', 'path_meta', 'meta_body_piece_names', 'sizes', 'bc_asset_path', 'material_stability']
    """

    def clean_res_key(s):
        tokens = s.split(" ")
        tokens[0] = tokens[0].replace("-", "")
        return " ".join(tokens).upper()

    # TODO - use an s3 provider for the schema artifacts below
    filters = None
    if style_id is not None:
        filters = AirtableConnector.make_key_lookup_predicate(style_id, "_recordid")
    elif styles is not None:
        if not isinstance(styles, list):
            styles = [styles]
        # allow different variations of what is ultimately CC6001 CTW70 SOMELU this format but allow SELF-- as a color with hyphen
        styles = [clean_res_key(s) for s in styles]
        filters = AirtableConnector.make_key_lookup_predicate(styles, "Key")

    res.utils.logger.debug(f"style filter {filters}")

    sfilter = styles

    airtable = res.connectors.load("airtable")
    # TODO: why is this not meta styles
    styles = airtable.get_airtable_table_for_schema_by_name(
        "meta.styles", filters=filters
    )

    bpieces = airtable["appa7Sw0ML47cA8D1"]["tbl4V0x9Muo2puF8M"].to_dataframe(
        fields=["Name", "Piece Name Code", "Type", "New Code", "Generated Piece Code"]
    )
    piece_lookup = dict(bpieces[["record_id", "Generated Piece Code"]].dropna().values)
    sizes_lu = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
    _slu = sizes_lu.to_dataframe(
        fields=["_accountingsku", "Size Normalized", "Size Chart"]
    )
    slu_chart = dict(_slu[["Size Chart", "Size Normalized"]].values)
    slu_inv = dict(_slu[["Size Normalized", "_accountingsku"]].values)

    mat_props = airtable.get_airtable_table_for_schema_by_name(
        "make.material_properties"
    ).set_index("key")
    mp = mat_props[mat_props["material_stability"].notnull()].drop(
        ["record_id", "__timestamp__", "order_key"], 1
    )
    mp = dict(zip(mp.index, mp.to_dict("records")))

    def contains_unstable_materials(m):
        if pd.notnull(m):
            for k, v in m.items():
                if not pd.isnull(v):
                    mt = v.get("material_stability")

                    if mt == "Unstable":
                        return True
        return False

    def try_piece_mapping(x):
        try:
            if pd.isnull(x):
                return None
            d = {
                piece_lookup[d["body_piece_id"]]: d.get("material_code", d.get("rank"))
                for d in json.loads(x)["style_pieces"]
            }

            d = {k: v for k, v in d.items() if v.lower() != "default"}
            # print(x)
            # print(d)
            return d if len(d) else None
        except Exception as ex:
            print(f"error with piece mappings {x}", repr(ex))
            return None

    def extract_secondary(row):
        mat = row.get("materials_used", [])
        if isinstance(mat, str):
            mat = mat.split(",")

        if isinstance(mat, list) and len(mat) > 1:
            other_materials = list(set(mat) - set([row.get("primary_material")]))
            if len(other_materials) > 1:
                res.utils.logger.warn(
                    f"There are more than three materials - how are we handing this {mat}"
                )
            return other_materials[0]

        # if we not told explicitly it seems we can get it from know piece mapping keys
        # but sometimes those are tank based and not material based
        pm = row.get("piece_mapping")
        if pm and not pd.isnull(pm):
            prim = row.get("material_code")
            for k, p in pm.items():
                if p != prim and k.lower() != "label" and p.lower() != "default":
                    return p

    def extract_label_material(row):
        pm = row.get("piece_mapping")
        if pm:
            prim = row.get("material_code")
            for k, p in pm.items():
                if k.lower() == "label":
                    return p

    def secondary_sku(row):
        pm = row.get("piece_mapping")
        if pm and not pd.isnull(pm):
            prim = row.get("material_code")
            if prim:
                for k, p in pm.items():
                    if p != prim and k.lower() != "label" and p is not None:
                        return row["res_key"].replace(str(prim), p)

    def repair_piece_mapping_for_ranks(row):
        """
        It seems sometimes we get ranks based piece mappings
        """
        primary_material = row.get("primary_material")
        secondary_material = row.get("secondary_material")

        pm = row.get("piece_mapping")

        if pd.isnull(pm):
            return pm

        def fix(v):
            if pd.notnull(v):
                if str(v).lower() == "primary":
                    return primary_material
                if str(v).lower() == "combo":
                    return secondary_material

        return {k: fix(v) for k, v in pm.items()}

    if return_source_styles:
        return styles

    res.utils.logger.info(
        f"Fetched {len(styles)} styles with cols {list(styles.columns)}"
    )

    styles["primary_material"] = styles["res_key"].map(lambda x: x.split(" ")[1])
    styles["piece_mapping"] = styles["style_pieces"].map(try_piece_mapping)

    styles["secondary_material"] = styles.apply(extract_secondary, axis=1)
    # update rank based material mapping if we know the secondary material
    # styles["piece_mapping"] = styles.apply(repair_piece_mapping_for_ranks, axis=1)
    # styles["label_material"] = styles.apply(extract_label_material, axis=1)
    styles["secondary_sku"] = styles.apply(secondary_sku, axis=1)

    if not allow_non_one_ready:
        styles = styles[styles["is_one_ready"] == 1]

        res.utils.logger.info(f"Filtered one-ready styles of length {len(styles)}")

    if len(styles) == 0:
        return styles

    def ensure_hyphen(s):
        if s[2] != "-":
            return f"{s[:2]}-{s[2:]}"

    styles["sku"] = styles["res_key"].map(ensure_hyphen)

    styles = styles[
        [
            "style_name",
            "res_key",
            "sku",
            "body_version",
            "body_code",
            "piece_mapping",
            "secondary_material",
            "primary_material",
            "print_type",
            "sizes",
            "record_id",
            "artwork_file_id",
            "style_cover_image",
            "created_at"
            # "meta_one_piece_attributes",
        ]
    ]
    styles["normed_size"] = styles["sizes"].map(
        lambda csl: [slu_chart.get(s.lstrip().rstrip()) for s in csl.split(",")]
        if not pd.isnull(csl)
        else None
    )

    # the size that comes from the list is in scale ->
    # styles["normed_size"] = styles["normed_size"].map(lambda x: slu_chart.get(x))

    # styles = styles[styles["piece_mapping"].map(lambda d: d is not None and len(d) > 0)]

    styles["piece_names"] = styles["piece_mapping"].map(
        lambda x: list(x.keys()) if not pd.isnull(x) else None
    )

    def alt_materials(row):
        """
        Here we are checking the DXF interface -> it may be 1:1 but for example look up the piece code without -BF or -S
        we may not want to do this but thinking about it here
        """
        pm = row.get("piece_mapping")
        if pm:
            d = {
                k.split("-")[0]: v
                for k, v in pm.items()
                if v == row.get("secondary_material") and v != "default"
            }
            return None if len(d) == 0 else d

    styles["alt_piece_mapping"] = styles.apply(alt_materials, axis=1)
    styles["alt_piece_names"] = styles["alt_piece_mapping"].map(
        lambda d: None if pd.isnull(d) else list(d.keys())
    )
    styles["color_code"] = styles["res_key"].map(lambda l: l.split(" ")[-1])

    # add material properties for all the materials in the style
    def add_material_properties(row):
        pm = row["res_key"].split(" ")[1]
        sm = row.get("secondary_material")
        materials = [pm] if not sm else [pm, sm]
        return {m: mp.get(m) for m in materials}

    styles["material_properties"] = styles.apply(add_material_properties, axis=1)

    styles["contains_unstable_materials"] = styles["material_properties"].map(
        contains_unstable_materials
    )
    # for debugging we may not want to remove these
    # the alt does a little cleaning
    styles = styles.drop(["piece_names"], 1)

    # size projection
    # styles = styles.explode("normed_size")
    # styles["size"] = styles["normed_size"].map(lambda s: slu_inv.get(s))
    # styles = styles.reset_index().drop("index", 1)
    def mapper(v):
        # if v not in slu_inv and "Size" in v:
        #     # we first 0 pad the size and then z pad it
        #     v = v.replace("Size ", "").rjust(2, "0").rjust(5, "Z")
        #     return v
        # now insisting its there except for exceptions
        return slu_inv[v] if v else None

    styles["size_map"] = styles["normed_size"].map(
        lambda ns: {mapper(v): v for v in ns} if isinstance(ns, list) else None
    )

    if force_version:
        res.utils.logger.debug(
            f"forcing the body version for the body to {force_version}"
        )
        styles["latest_body_version"] = styles["body_version"]
        styles["body_version"] = int(force_version.replace("v", ""))

    return styles


def get_style_cost_table_by_sku(sku, size=None, pivot_table=False):
    """ """
    from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient

    ql = ResGraphQLClient()

    q = f"""query($code: String!) {{
      style(code: $code)  {{
        onePrices {{
          size {{
            id
            name
            code
          }}
          cost
          price
          margin
          priceBreakdown {{
            item
            category
            rate
            quantity
            unit
            cost
          }}
        }}
      }}
    }}"""

    try:
        df = pd.DataFrame(
            ql.query_with_kwargs(q, code=sku)["data"]["style"]["onePrices"]
        )
        df = df.explode("priceBreakdown").reset_index(drop=True)
        df["size"] = df["size"].map(lambda x: x["code"])
        df = res.utils.dataframes.expand_column(df, "priceBreakdown").drop(
            "priceBreakdown", 1
        )

        if size:
            df = df[df["size"] == size]
        if pivot_table:
            df = df.pivot(
                ["size", "cost"], ["priceBreakdown_item", "priceBreakdown_category"]
            )

        return df.reset_index(drop=True)
    except Exception as ex:
        res.utils.logger.warn(f"Failed to get costs for style because {repr(ex)}")
        return None


def get_style_cost_table(record_id, size=None, pivot_table=False):
    """
    Using the airtable record id that the graph expects, query the costs
    if size specified we filter by size in the accu size format e.g.

    Example:
     get_style_cost_table('rec00gWrzVGRPvg6Y', size='1ZZXS')

    """
    from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient

    key = record_id
    ql = ResGraphQLClient()

    q = f"""query($id: ID!) {{
      style(id: $id) {{
        onePrices {{
          size {{
            id
            name
            code
          }}
          cost
          price
          margin
          priceBreakdown {{
            item
            category
            rate
            quantity
            unit
            cost
          }}
        }}
      }}
    }}"""

    try:
        df = pd.DataFrame(ql.query_with_kwargs(q, id=key)["data"]["style"]["onePrices"])
        df = df.explode("priceBreakdown").reset_index(drop=True)
        df["size"] = df["size"].map(lambda x: x["code"])
        df = res.utils.dataframes.expand_column(df, "priceBreakdown").drop(
            "priceBreakdown", 1
        )

        if size:
            df = df[df["size"] == size]
        if pivot_table:
            df = df.pivot(
                ["size", "cost"], ["priceBreakdown_item", "priceBreakdown_category"]
            )

        return df.reset_index(drop=True)
    except Exception as ex:
        res.utils.logger.warn(f"Failed to get costs for style because {repr(ex)}")
        return None
