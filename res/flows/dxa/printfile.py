# from requests_toolbelt import ImproperBodyPtion

import res
import os

from res.flows.make.nest.pack_pieces import (
    DPI_SETTING,
    DEFAULT_HEIGHT as DEFAULT_NEST_HEIGHT,
)
from res.flows.make.nest.utils import get_nest_key_from_argo_job_key, flag_asset
from res.learn.optimization.nest import (
    evaluate_nest_v1,
    DEFAULT_WIDTH as DEFAULT_NEST_WIDTH,
)
from res.flows.FlowContext import FlowException
from res.media.images.geometry import (
    alpha_hulls,
    swap_points,
    cascaded_union,
    Polygon,
    scale as geom_scale,
)
from res.flows import FlowContext
from res.flows.FlowContext import NamedFlowAsset
from res.media import images
from res.learn import optimization

from tenacity import retry, wait_fixed, stop_after_attempt
from pathlib import Path
import numpy as np
from shapely.geometry import Polygon
from shapely.wkt import loads as shapely_loads
import itertools
import pandas as pd


META_ONE_ROOT = "s3://meta-one-assets-prod/styles/meta-one"


"""

THIS IS THE V1 version of nesting. 
For details see: https://coda.io/d/Platform-Documentation_dbtYplzht1S/V1-Nesting-Non-piece-level_sultn#_luOB7


This flow takes instructions to prepare print files for meta ones and produces print file(s)
The default flow is a simple linear flow with default parameters
In general we can load different state machine topologies and different experiments 
This is done using the flow context

Flows take an event structure such as this one
              {
                "apiVersion": "v0", 
                "kind": "resFlow", 
                "metadata": {"name": "dxa.printfile", "version": "exp1"}, 
                "args": 
                {
                  "assets": [
                    { "value": "s3://one-meta-artifacts/testb-testm-testc/print/source.png" }, 
                    { "value" : "s3://one-meta-artifacts/testb-testm-testc/print/source.png" }
                            ], 
                  "jobKey": "job1", 
                  "material": "LTCSL", 
                }
              }
       
"""


MARKERS_BASE = "app1FBXxTRoicCW8k"
MARKERS_TABLE = "tblD1kPG5jpf6GCQl"
COMP_WIDTH_COL_NAME = "Locked Width Digital Compensation (Scale)"
COMP_LENGTH_COL_NAME = "Locked Length Digital Compensation (Scale)"
OFFSET_BUFFER = "Offset Size (Inches)"

PAPER_MARKER_COMP_WIDTH_COL_NAME = "Paper Marker Compensation (Width) FORMULA"
PAPER_MARKER_COMP_LENGTH_COL_NAME = "Paper Marker Compensation (Length) FORMULA"

MATERIAL_COL_NAME = "Material Code"
CUTTABLE_WIDTH_COL_NAME = "Printable Width (In)"

MAX_PIXEL_PNG_LENGTH = 650000
MAX_ALLOWED_PIECES = 650

MAX_ASSETS = 12

# Airflow tables for error logging.
BASE_DXA_ONE_MARKERS = "appqtN4USHTmyC6Dv"
TABLE_ASSEMBLY_FAILURE_REVIEW = "tblpDzTdXTJFFx9zP"
BASE_RES_MAGIC_PRINT = "apprcULXTWu33KFsh"
TABLE_PRINT_ASSETS_FLOW = "tblwDQtDckvHKXO4w"
TABLE_NESTS = "tbl7n4mKIXpjMnJ3i"

DPMM = 1 / 0.084667


def pixels_to_yards(p):
    try:
        inches = p / DPI_SETTING
        yards = inches / 36

        return int(np.ceil(yards))
    except:
        return -1


def augment_nested_assets(
    data,
    save=False,
    nested_ones_table_name="nested_ones",
    nesting_path_root="s3://res-data-production/flows/v1/dxa-printfile/expv2/extract_parts",
):
    """
    now when we begin to nest some assets, we can save data about what we are nesting
    this stores the payload and resolves the sku
    it could add other things but that should be enough
    """
    from res.connectors.snowflake import CachedWarehouse
    from res.flows.make.production_requests import get_airtable_production_orders

    s3 = res.connectors.load("s3")

    def count_pieces(key):
        """
        We can use the number of files nested on the path
        BUT - this must be called AFTER nesting for this to work and this is the only figure we need after nesting completes - or at least after split pieces
        In future
         when we prep pieces, nesting would know this when called too
        """

        try:
            return ResNestV1.from_production_key(key).num_pieces
        except:
            return None

    def cached_production_sku_resolver(keys=None):

        sku_data = CachedWarehouse(
            prod_data_table,
            key="one_number",
            missing_data_resolver=get_airtable_production_orders,
        )
        if keys:
            res.utils.logger.info(f"Filtering lookup by keys {keys}")
            d = sku_data.lookup_keys(keys)
        else:
            d = sku_data.read()

        d = dict(d[["one_number", "sku"]].values)

        def f(o):
            return d.get(o)

        return f

    # in the generator of the nest we are going to stage and pipe the nest jobs sku nests

    res.utils.logger.info("exploding assets")
    recs = data.explode("assets")

    res.utils.logger.info(f"Processing {len(recs)} exploded assets")

    recs = recs[["assets", "jobKey", "material", "compensation_x", "compensation_y"]]
    recs["marker_path"] = recs["assets"].map(lambda x: x.get("value"))
    recs["marker_key"] = recs["assets"].map(
        lambda x: x.get("value").split("/")[-1].split(".")[0]
    )
    recs["one"] = recs["assets"].map(lambda x: x.get("key"))
    recs["piece_types"] = "self"
    recs["meta_one_key"] = None
    recs["piece_count"] = -1  # recs["marker_key"].map(count_pieces)

    # for easy re-use if we are doing a big batch just fetch the entire snowflake - this doesnt really happen that much
    keys = list(recs["one"]) if len(recs) < 500 else None

    res.utils.logger.debug(f"loading sku resolver with keys {keys}")
    sku_resolver = cached_production_sku_resolver(keys)

    res.utils.logger.debug(f"applying sku resolver")
    recs["sku"] = recs["one"].map(sku_resolver)
    res.utils.logger.info(f"one")

    # this will exist in ENV.MAKE.NESTED_PRODUCTS
    recs = recs.drop("assets", 1).rename(
        columns={"jobKey": "job_key", "one": "one_number"}
    )
    recs["created_at"] = recs["job_key"].map(
        lambda x: "-".join(x.split("-")[2:][:3]) + " " + ":".join(x.split("-")[2:][3:])
    )
    recs["created_at"] = pd.to_datetime(recs["created_at"], errors="coerce")

    # return recs

    # these are the ones that should have valid keys
    recs = recs[recs["created_at"].notnull()]
    # recs = recs[recs["piece_count"] > 0]
    recs = recs[recs["sku"].notnull()]

    recs["body_code"] = recs["sku"].map(lambda x: x.split(" ")[0])
    recs["body_code"] = recs["body_code"].map(lambda x: f"{x[:2]}-{x[2:]}")

    recs["size_code"] = recs["sku"].map(lambda x: x.split(" ")[3])

    def make_key(row):
        """
        todo: how we describe a unique partition of the sku
        for the moment its really just the one and the material we care about
        """
        d = {
            k: v
            for k, v in dict(row).items()
            if k in ["one_number", "material", "piece_type", "rank", "piece_code"]
        }

        dhash = res.utils.hash_dict(d)
        return res.utils.res_hash(dhash.encode())

    res.utils.logger.info(f"Making keys for record set of length {len(recs)}")

    recs["key"] = recs.apply(make_key, axis=1)

    def mak_bcs_key(row):
        return f"{row['body_code']}-{row['material']}-{row['size_code']}"

    recs["bcs_key"] = recs.apply(mak_bcs_key, axis=1)

    recs = recs.sort_values("created_at").drop_duplicates(subset=["key"], keep="last")

    res.utils.logger.info("Looking for piece counts on the reduced set...")
    recs["piece_count"] = -1  # recs["job_key"].map(count_pieces)
    res.utils.logger.info("piece counts added")

    if save:
        """
        the key here should be not on a ONE but on a slice of a ONE
        a slice is something like a material code, piece type or piece name
        hash-> uri, piece types, material, piece_ids
        piece ids are a future thing but today would be the stem of the uri
        slight issue here is if the uri changes due to a re-issue so may omit this for now
        rank can be passed too as ONE+RANK should define the set

        Is healing logic - we might get away with piece name supplied or integer stem
        For future we should add a rank descriptor in the payload
        """
        res.utils.logger.info(f"saving nested ones")
        # now we have nested skus in the warehouse
        CachedWarehouse(nested_ones_table_name, key="key", df=recs).write()
        res.utils.logger.info(f"done")

        # TODO: - we could also update the prod records to link our nest key there but lets not for now
        # we can still join it in snowflake or look it up if we need it so lets wait to see who's asking

    return recs


@res.flows.flow_node_attributes(
    memory="60Gi",
)
def update_old_nest_stats(event, context={}):
    """
    This is a bootsrapper we can run on the server - it will fetch whatever old nests we have
    and used our latest method for storing metadata per nested one.
    As we can change how we do this sometimes we need to re=process
    """

    from ast import literal_eval

    with FlowContext(event, context) as fc:
        watermark = fc.args.get("watermark", 30)

        s3 = res.connectors.load("s3")

        res.utils.logger.info(
            f"Reading old nests and using the latest way to store nested stats"
        )
        files = list(
            s3.ls(
                "s3://res-data-production/flows/v1/dxa-printfile/expv2/.meta",
                modified_after=res.utils.dates.relative_to_now(watermark),
            )
        )

        data = [s3.read(f).get("args") for f in files]

        data = pd.DataFrame(data)

        res.utils.logger.info(
            "saving the sample - reuse next time. Will then augment and cache"
        )

        data.reset_index().to_csv(
            "s3://res-data-platform/samples/data/old_nest_payloads_180.csv"
        )

        # data = s3.read("s3://res-data-platform/samples/data/old_nest_payloads.csv")

        try:
            data["assets"] = data["assets"].map(literal_eval)
        except:
            pass

        augment_nested_assets(data, save=True)


@retry(wait=wait_fixed(3), stop=stop_after_attempt(3))
def _try_get_material_comp(fc, material):
    """
    utility method to load params that are stored in airtable
    """
    try:
        airtable_connector = fc.connectors["airtable"]
        markers = airtable_connector[MARKERS_BASE][MARKERS_TABLE]

        # get the most recent non null entry in this table??
        mks = (
            # add cuttable width
            markers.to_dataframe(
                fields=[
                    COMP_LENGTH_COL_NAME,
                    COMP_WIDTH_COL_NAME,
                    PAPER_MARKER_COMP_WIDTH_COL_NAME,
                    PAPER_MARKER_COMP_LENGTH_COL_NAME,
                    CUTTABLE_WIDTH_COL_NAME,
                    MATERIAL_COL_NAME,
                    OFFSET_BUFFER,
                ],
                filters="FIND('" + material + "', {" + MATERIAL_COL_NAME + "})",
            )
            .sort_values("__timestamp__")
            .dropna()
            .rename(
                columns={
                    # TODO: what are the mappings for these?
                    COMP_LENGTH_COL_NAME: "stretch_y",
                    COMP_WIDTH_COL_NAME: "stretch_x",
                    PAPER_MARKER_COMP_LENGTH_COL_NAME: "pm_stretch_y",
                    PAPER_MARKER_COMP_WIDTH_COL_NAME: "pm_stretch_x",
                    CUTTABLE_WIDTH_COL_NAME: "material_cuttable_width",
                    OFFSET_BUFFER: "offset_buffer_inches",
                }
            )
            .iloc[-1]
        )

        mks = dict(mks)
        # broken abstraction
        # set the output width to be the read in inches times DPI (temp default 55 inches)
        mks["output_bounds_width"] = (
            mks.get("material_cuttable_width", 55) * DPI_SETTING
        )

        mks["MARKER_RECOMP_LENGTH"] = mks["pm_stretch_y"] / mks["stretch_y"]
        mks["MARKER_RECOMP_WIDTH"] = mks["pm_stretch_x"] / mks["stretch_x"]

        res.utils.logger.info(f"Loaded material info  {mks}")

        return mks

    except Exception as ex:
        raise FlowException("Failed to get material data", ex, fc)


def default(event, context):

    # making this below, below are just notes to absorb into docs
    return extract_image_elements(event, context)


def _determine_image_key_from_path(path, asset=None, method="stem"):
    """
    We can use a rule to determine what keys to use from the source images such as the file name or parent directory
    We could also use a ash or even pass in a lambda
    """
    # for a meta marker this is the key we will use for  now
    if META_ONE_ROOT in path:
        order_key = asset["key"]
        white_list = ["key", "value", "material_codes", "piece_types"]
        H = res.utils.hash_dict_short(
            {k: v for k, v in asset.items() if k in white_list}
        )
        return "-".join(path.split("/")[-4:]) + "-" + order_key + "-" + H
    # path = Path(path)

    # for healing we have an integer part after a guid part
    # we can rehash this using a trick for copy otherwise the folder is not a root
    # the convention should be to copy the files to a new has location using hash of UUID/n.png

    # stem for file or folder - the last part without any file extension
    if is_healed_asset(path):
        # this returns UUID/n.png as a valid key for filtering S3 data
        k = "/".join(path.split("/")[-2:])
        # the key should be extensionless
        return k.split(".")[0]

    return path.parts[-2] if method == "folder" else path.split("/")[-1].split(".")[0]


def get_image_sizes_and_limits(event, images, **kwargs):
    def mem_from_asset(a):
        default_memory = a.get("memory", "50Gi")
        if "marker_dimensions" in a:
            dims = a["marker_dimensions"]
            x, y = dims["width"], dims["height"]
            area = x * y
            # actually we could control this better with channel info but this is a temporary thing so we treat as a hint
            factor = 3
            memory = int(area / 10**8 * factor)
            if memory > 50:
                return f"{memory}Gi"

        return default_memory

    with FlowContext({}, {}) as fc:
        for i, d in enumerate(images):
            mem = mem_from_asset(d)
            d.update({"memory": mem})

        return images


def _pass_through(collection, **kwargs):
    for c in collection:
        yield c


def asset_stem_of(p):

    if is_healed_asset(p):
        """
        the stem is not what we want here in the case of healing
        """
        return "/".join([c.split(".")[0] for c in p.split("/")[-2:]])

    return Path(p).stem


def _evaluate_nesting(event, nesting, try_image_keys, requested_pieces):
    job_key = event.get("args").get("jobKey")
    bounds = _get_request_bounds_from_event(event)

    res.utils.logger.info(f"eval nesting on image keys {try_image_keys}")
    nested_height = None
    if nesting is not None:
        nesting["area"] = float(
            nesting["nested"].map(images.geometry.ndarray_area).sum()
        )
        max_y = int(nesting["max_nested_y"].max())
        min_y = int(nesting["min_nested_y"].min())
        nested_height = max_y - min_y

    nest_image_keys_requested = event.get("args").get("assets")

    outpath = FlowContext(event, {}).get_node_path_for_key(
        "perform_nest_apply", key=job_key
    )

    # TODO: this stem thing is really bad. need to change
    def match(d, try_image_keys):
        path = d["value"]
        if META_ONE_ROOT in path:
            H = _determine_image_key_from_path(path, d)
            return H in try_image_keys

        return asset_stem_of(d["value"]) in try_image_keys

    omitted_assets = [
        d for d in nest_image_keys_requested if not match(d, try_image_keys)
    ]

    nested_assets = [d for d in nest_image_keys_requested if match(d, try_image_keys)]

    result = {
        "nested_assets": nested_assets if nesting is not None else [],
        # if we cannot nest return all as ignored
        "omitted_assets": nest_image_keys_requested
        if nesting is None
        else omitted_assets
        if nesting is None
        else omitted_assets,
        "print_file": outpath,
        "print_file_ready": False,
        "evaluation": {
            "nested_area": nesting["area"].sum() if nesting is not None else 0,
            "request_width": bounds["output_bounds_width"],
            "request_length": bounds["output_bounds_length"],
            "total_area": bounds["output_bounds_width"]
            * bounds["output_bounds_length"],
            "nested_length": nested_height,
            "num_pieces_requested": len(requested_pieces),
            "num_pieces_nested": len(nesting) if nesting is not None else None,
        },
    }

    for k in ["request_width", "nested_length", "request_length"]:
        result["evaluation"][f"{k}_yards"] = pixels_to_yards(result["evaluation"][k])

    return result


def _get_request_bounds_from_event(event):
    with FlowContext(event, {}) as fc:
        args = event.get("args", {})
        job_key = args.get("jobKey")
        material = args.get("material", None)
        output_bounds_length = args.get("output_bounds_length", DEFAULT_NEST_HEIGHT)
        marker_params = _try_get_material_comp(fc, material)

        output_bounds_width = marker_params.get(
            CUTTABLE_WIDTH_COL_NAME, DEFAULT_NEST_WIDTH
        )

        bounds = {
            "output_bounds_width": output_bounds_width,
            "output_bounds_length": output_bounds_length,
        }

        return bounds


# on an exception - write the info to airtable because it means the pieces are hosed.
def make_exception_callback(asset, fc):
    def callback(ex):
        if os.getenv("RES_ENV") == "production":
            error_message = ex.args[0]
            if isinstance(ex, ValueError) or isinstance(ex, FileNotFoundError):
                flag_asset(
                    asset["key"],
                    asset["asset_id"],
                    asset["value"],
                    fc._task_key,
                    f"{error_message} (argo job key {fc._task_key})",
                )

    return callback


def marker_params_from_event(fc):
    event = fc._event
    material = fc.args.get("material")
    marker_params = _try_get_material_comp(fc, material)
    event_args = event.get("args", {})
    if "compensation_x" in event_args:
        custom_x = event_args.get("compensation_x")
        res.utils.logger.debug(f"Using a compensation value for x {custom_x}")
        marker_params["stretch_x"] = custom_x
    if "compensation_y" in event_args:
        custom_y = event_args.get("compensation_y")
        res.utils.logger.debug(f"Using a compensation value for y {custom_y}")
        marker_params["stretch_y"] = custom_y

    return marker_params


def _process_as_meta_marker(asset, marker_key, fc):
    """
    We satisfy the upstream contract using the meta object instead
    If the meta marker has been expored successfully for a style the client can choose to use the meta marker for nesting

    - We should copy the pngs with annotations to the extacted parts [use the meta marker labeller; test min one label]
    - We should copy the boundaries to the folder used for nesting shapes using the correct convention in xy
    - Update metrics
    - For healing the unique piece name should be used i healing aps

    - we could be smarter in future and use buffered outlines for nesting instead of astm outlines when confidence is low
    - THE KEY for a saved piece should be f(ONE,meta-marker) which makes ot unique and can also be generated on an order
      - if we can resolve a ONE number earlier all the better

    """

    from res.flows.meta.marker import MetaMarker

    res.utils.logger.info(
        f"Processing nest request from asset {asset} and key {marker_key}"
    )
    marker = MetaMarker.from_restNestv1_asset(asset)
    res.utils.logger.info(
        f"loaded the marker from {marker._home_dir} of piece length {len(marker._data)}"
    )

    def get_filtered_outlines(marker_ref, **kwargs):
        # TODO implement m.get_piece_outlines_with_confidence(threshold=100)
        # for ol in marker_ref.piece_outlines:
        # this is slower but we have to take of the buffered image and any image with physical notches
        # to nest properly in general
        for name, ol in marker_ref.named_notched_piece_image_outlines:
            res.utils.logger.debug(f"Loaded outline {name}")
            # make res nest compatible and return - we return the hulls of the object for nesting because we dont want image space objects
            yield NamedFlowAsset(name, np.array(swap_points(ol).convex_hull.boundary))
            # yield np.array(ol)

    def get_filtered_notched_outlines(marker_ref, **kwargs):
        # TODO implement m.get_piece_outlines_with_confidence(threshold=100)
        # this is subtly the internal raw notched outlines which are not color/buffered pieces
        # compensation should not be applied here but we can now or later apply paper marker compensation
        for name, ol in marker_ref.get_named_notched_outlines(compensate=False):
            res.utils.logger.debug(
                f"Loaded raw outline - compensation was applied: {name}"
            )

            # make res nest compatible and return
            yield NamedFlowAsset(name, np.array(swap_points(ol)))
            # yield np.array(ol)

    def get_labelled_parts(marker_ref, **kwargs):
        for name, image in marker_ref.named_labelled_piece_images:  # labelled_
            res.utils.logger.debug(f"Loaded labelled piece image: {name}")
            yield NamedFlowAsset(name, image)

    # TODO: we can probe the number of pieces we already have and skip if the cache is valid

    fc.apply(
        node="extract_alpha_hulls",
        func=get_filtered_outlines,
        data=marker,
        key=marker_key,
        exception_callback=make_exception_callback(asset, fc),
    )

    fc.apply(node="extract_parts", func=get_labelled_parts, data=marker, key=marker_key)

    try:
        res.utils.logger.info(f"notched outlines generation....")
        fc.apply(
            node="notched_outlines",
            func=get_filtered_notched_outlines,
            data=marker,
            key=marker_key,
        )
    except Exception as ex:
        res.utils.logger.warn(
            f"Failed an experimental step of finding notched outlines: {repr(ex)}"
        )

    res.utils.logger.info(
        f"generated resources for flow context {fc.key} and marker {marker_key}"
    )


def _lookup_sku_from_ONEs(one_numbers):
    """
    we need to resolve the sku from the ONE and we use a cache for this
    """
    from res.connectors.airtable import AirtableConnector

    if not isinstance(one_numbers, list):
        one_numbers = [one_numbers]

    dynamo = res.connectors.load("dynamo")
    airtable = res.connectors.load("airtable")

    result = dynamo["airtable_cache"]["airtable_make_one_production"].get(one_numbers)

    if len(result) == 0:
        # if we did not find it in the cache lookup airtable make one production
        result = airtable.get_airtable_table_for_schema_by_name(
            "make.production_requests",
            filters=AirtableConnector.make_key_lookup_predicate(
                one_numbers, "Order Number v3"
            ),
        )

    if len(result):
        return dict(result[["one", "sku"]].values)

    return {}


def _temp_rewrite_event_for_metamarkers(event):
    """
    if the flows upstream are not ready to enable meta markers we can enable a lookup for configured skus
    skus are used rather than bodies or styles as we make meta markers at the BMC-Size level which is a sku
    """

    from res.flows.meta.marker import MetaMarker

    try:
        assets = event.get("assets")

        # get a config entry for this event and env - this will be for the dxf.printfile
        config = FlowContext.get_config_for_event(
            {"metadata": {"name": "dxa-printfile"}}
        )

        one_numbers = [a["key"] for a in assets]
        sku_lookup = _lookup_sku_from_ONEs(one_numbers)

        for a in assets:
            sku = sku_lookup.get(a["value"])
            if sku in config.get_config_value("metamarker_skus") or []:
                meta_marker = MetaMarker.search_cache(sku=sku)
                if meta_marker:
                    meta_marker = meta_marker.iloc[0]._home_dir
                    res.utils.logger.info(
                        f"Rewriting metamarker for {a['key']} with {meta_marker}"
                    )
                    a["value"] = meta_marker

        # update the list object
        event["assets"] = assets

    except Exception as ex:
        res.utils.logger.info(
            "Failed in an attempt to rewrite the marker with a meta marker"
        )

    return event


# def prep_for_healing_requests(event):
#     """
#     we need to refactor healing does not quite work like this because if the hashing of work folders

#     """

#     temp = event["args"]["assets"]

#     assets = []
#     for a in temp:

#         #if its a healing, rewrite the extracted parts and outlines to a new location and replace the value with the new value UUID/o.stem

#         assets.append(a)

#     event["args"]["assets"] = assets

#     return event


def is_healed_asset(a):

    comp = a.split("/")
    file = comp[-1].split(".")[0]
    try:

        fid = int(float(file))

        if "-" in comp[-2]:

            res.utils.logger.debug(f"{a} is a healing request")
            return True
    except:
        return False

    return False


def healed_asset_marker_dir(p):
    """
    TODO: determine meta one context too. below works for intelligent one markers
    """
    uuid = p.split("/")[-2]

    return f"s3://resmagic/uploads/{uuid}.png"


def extract_image_elements(event, context):
    """
    This is the entry point for the nest so we are going to store the stuff in snowflake here
    """
    # event = _temp_rewrite_event_for_metamarkers(event)
    s3 = res.connectors.load("s3")
    args = event["args"]
    apply_piece_extraction = args.get("isFullMarker", True)
    add_cutline = args.get("addCutline", False)
    make_thumbnail = args.get("makeThumbnail", False)
    style_code = args.get("styleCode", "test").replace(" ", "_")
    cache_enabled = args.get("useCache", False)
    check_piece_contains = args.get("check_piece_contains", False)
    min_part_area = args.get("min_part_area", 300)

    marker_params = marker_params_from_event(FlowContext(event))
    res.utils.logger.debug(
        f"Max cuttable width is determined from {marker_params}. Will fail parsing when this is exceeded"
    )

    # prepare healing case

    # assets are a dict and we pull out the value
    image_keys = [
        _determine_image_key_from_path(p["value"], p, method="stem")
        for p in event["args"]["assets"]
    ]

    images.text.ensure_s3_fonts()
    images.icons.extract_icons()

    with FlowContext(event, node="split_pieces", data_group=args.get("material")) as fc:
        for asset in event["args"]["assets"]:
            path = asset["value"]
            exception_callback = make_exception_callback(asset, fc)
            if is_healed_asset(asset["value"]):
                # skip splitting healed assets - we dont need to - but we will need a key trick
                # it could be that we do a file copy here to a new location with the assumption that the new image key will be generated
                if s3.exists(asset["value"]):
                    res.utils.logger.info("the asset already exists no need to split")
                    continue
                # else:

                #     path = healed_asset_marker_dir(path)
                #     # test heal meta one
                #     res.utils.logger.info(
                #         f"need to re-split asset removed from cache at location {path}"
                #     )

            fc.publish_asset_status(asset, "nesting", "", "IN_PROGRESS")

            expected_piece_count = asset.get("number_of_pieces")
            # piece_split_mask = non_white_mask
            # if ??:
            #     piece_split_mask = non_transparent_mask

            # temp caching solution: if we have the hulls for this image, continue
            # todo - if fc.completed_node_task(final='extract_alpha_hulls') skip
            all_bounds = [
                # we are going to pass XY format to nesting algo
                data[["column_1", "column_0"]].values
                for data in fc.get_node_data("extract_alpha_hulls", keys=image_keys)
            ]
            if len(all_bounds) > 0:
                if cache_enabled:
                    res.utils.logger.debug(
                        "we already have the hulls for this node, skipping as useCache is set to True"
                    )
                    continue
                else:
                    # if we have a marker creation date we use it as a watermark otherwise we just go back 5 hours
                    cache_watermark = res.utils.dates.utc_hours_ago(5)
                    # actually we should only cache for a fixed time to always be able to repair
                    # cache_watermark = asset.get(
                    #     "marker_created_datetime", cache_watermark
                    # )
                    if isinstance(cache_watermark, str):
                        cache_watermark = res.utils.dates.parse(cache_watermark)

                    res.utils.logger.debug(
                        f"Using the watermarker {cache_watermark} of type {type(cache_watermark)} for asset {path} when clearing caches"
                    )
                    # if the asset has a creation data for the asset we will use that instead

                    # for correcting existing assets we should force an overwrite of whatever we have done
                    for n in [
                        "extract_alpha_hulls",
                        "extract_parts",
                        "extract_part_outlines",
                    ]:
                        fc.clear_node_data(
                            n, keys=image_keys, older_than=cache_watermark
                        )

            #######################################################################

            ###
            ## adding meta marker method for when meta markers exist in place of intelligent markers
            ###
            key = _determine_image_key_from_path(path, asset=asset, method="stem")

            if META_ONE_ROOT in path:
                _process_as_meta_marker(asset, key, fc)
                continue

            if is_healed_asset(asset["value"]):
                key = key.split("/")[0]
                path = healed_asset_marker_dir(path)
                res.utils.logger.debug(
                    f"For the healing, where no cached data, we are regenerating expensively from the marker at {path}"
                )
                # for healing or generate the key should be without a tail

            image = fc.connectors["s3"].read(path)

            meta_folder = fc.get_node_path_for_key("masks", key)

            def logging_function(path, data):
                if len(data) > 0:
                    res.utils.logger.info(f"saving data to {meta_folder}/{path}")
                    fc.connectors["s3"].write(f"{meta_folder}/{path}", data)

            if apply_piece_extraction:
                # image is now [parts] but reusing the var to limit mem use/ help gc

                if make_thumbnail:
                    _ = fc.apply(
                        node="transparent_style_thumbnail",
                        func=images.outlines.transparent_thumbnail,
                        data=image,
                        key=style_code,
                    )

                image = fc.apply(
                    node="extract_parts",
                    func=images.outlines.parts,
                    data=image,
                    key=key,
                    mask_output_path=fc.get_node_path_for_key("masks", key)
                    + "/mask.png",
                    check_piece_contains=check_piece_contains,
                    min_part_area=min_part_area,
                    output_bounds_width=marker_params.get("output_bounds_width"),
                    stretch_x=marker_params.get("stretch_x"),
                    log_file_fn=logging_function,
                    exception_callback=exception_callback,
                    # mask_to_use=piece_split_mask,
                )

            def enumerate_outlines(data, **kwargs):
                for g in fc.connectors["s3"].read(
                    f"{meta_folder}/piece_outlines.feather"
                )["geometry"]:
                    # swap points to recreated what comes from the outlines mapped to what is exected from nesting input convention???
                    yield np.array(swap_points(shapely_loads(g)))

            # for this flow if we calc keypoints up front we can pass them to other functions as a cache
            # TODO: determine keypoints for label placements etc.

            # if add_cutline:
            #     image = fc.apply(
            #         # TODO: think about how flows work e.g. this cutline will "put" images into the node for the print file writer
            #         node="draw_part_outlines",
            #         # this is a way to save the outlines where they would be saved without actually computing them
            #         func=images.outlines.draw_part_outlines
            #         if apply_piece_extraction
            #         else _pass_through,
            #         ##################
            #         data=image,
            #         key=key,
            #         max_notch_width=50,
            #     )

            outlines = fc.apply(
                node="extract_part_outlines",
                func=enumerate_outlines,
                data=None,
                key=key,
                exception_callback=exception_callback,
            )

            hulls = fc.apply(
                node="extract_alpha_hulls",
                func=alpha_hulls,
                data=outlines,
                key=key,
                alpha=0,
                exception_callback=exception_callback,
            )

            chk_sum = np.sum(
                [p.get("number_of_pieces", 0) for p in event["args"]["assets"]]
            )

            generated_hulls = list(hulls)
            generated_hull_count = len(generated_hulls)

            res.utils.logger.info(f"The hull count is {generated_hull_count}!")
            res.utils.logger.info(f"The asset request check sum is {chk_sum}!")

            if int(chk_sum) != int(generated_hull_count):
                res.utils.logger.warn(
                    f"There are an incorrect number of pieces: expected a total from parsing of {chk_sum} but got {generated_hull_count}"
                )
                # raise Exception("Incorrect number of pieces")

            if expected_piece_count:
                if int(expected_piece_count) != int(generated_hull_count):
                    res.utils.logger.warn(
                        f"There are an incorrect number of pieces: expected a total from the metadata of {expected_piece_count} but got {generated_hull_count}"
                    )
                    # raise Exception("Incorrect number of pieces")

            # this is a generic way to count anything in a flow
            fc.metric_incr("counted", status="pieces", inc=generated_hull_count)


def estimate_memory(event, context):
    with FlowContext(event, context) as fc:
        image_source_node = "extract_parts"  # "draw_part_outlines"
        image_keys = [
            _determine_image_key_from_path(p["value"], p, method="stem")
            for p in event["args"]["assets"]
        ]
        image_count = fc.get_node_data_count("extract_alpha_hulls", keys=image_keys)

        length = image_count
        memory = "32Gi"
        if length > 100:
            memory = "64Gi"

        return {"length": length, "memory": memory}


def parse_args(event, context):
    args = event.get("args", {})
    job_key = args.get("jobKey")
    # TODO: should we assert jobKey
    material = args.get("material", None)
    buffer = args.get("buffer", 75)

    # The min buffer is 75 but can be set to more
    buffer = max(75, buffer)

    shrink_output_if_possible = args.get("shrink_output_if_possible", True)
    restricted_keys = args.get("image_key_restriction", [])
    output_bounds_length = args.get("output_bounds_length", DEFAULT_NEST_HEIGHT)
    # try get the param in inches
    output_bounds_length_inches = args.get("output_bounds_length_inches")
    if output_bounds_length_inches is not None:
        output_bounds_length = int(output_bounds_length_inches * DPI_SETTING)

    if output_bounds_length > MAX_PIXEL_PNG_LENGTH:
        res.utils.logger.info(
            f"LIMITING LENGTH TO CONFIGURED MAX {MAX_PIXEL_PNG_LENGTH}"
        )
        output_bounds_length = MAX_PIXEL_PNG_LENGTH
    #############################
    nest_retries = args.get("nest_retries_with_remove", 7)
    write_file = args.get("writeFile", True)
    # these are determined from the job image list - these are important as they are the job's request data
    # the keys are relative to the job and can be cached e.g. pieces-of original image
    image_keys = [
        _determine_image_key_from_path(p["value"], p, method="stem")
        for p in event["args"]["assets"]
    ]

    # TODO: move filter to determine image keys from path so no ambiguity about stems etc
    # create a hash of the request
    if restricted_keys:
        raise Exception("restricted image keys not currently supported")
        image_keys = [Path(f).stem for f in restricted_keys]

    return {
        "material": material,
        "job_key": job_key,
        "nest_retries": nest_retries,
        "buffer": buffer,
        "output_bounds_length": output_bounds_length,
        "shrink_output_if_possible": shrink_output_if_possible,
        "image_keys": image_keys,
        "write_file": write_file,
        "header_image": args.get(
            "header_image",
            "s3://res-data-platform/samples/images/ColorDDetection-01.png",
        ),
    }


def shift_to_origin(arr):
    top_pt = [arr[:, 0].min(), arr[:, 1].min()]
    return arr - top_pt


def _save_style_evaluation(data, result, fc):
    """
    the data are the one level/pieces - what we really need are just they keys
    """

    from res.flows.meta.marker import (
        resolve_for_ordered_assets,
        _aggregate_metamarker_nesting_stats,
    )

    def aggregate_to_dict(df):
        """
        count the meta marker assets that we could resolve
        """
        df["asset_marker_count"] = len(df)

        return df.to_records("dict")[0]

    try:
        nested_one_numbers = data["one_number"].unique()
        nested_assets = [a for a in fc.assets if a["key"] in nested_one_numbers]

        # we need to dertermine that the metamarlers would be for the given orders if they exist
        meta_markers = resolve_for_ordered_assets(nested_assets)

        # we determine a particular summary format for the meta marker statistics
        # not clear who really owns that
        aggregate = _aggregate_metamarker_nesting_stats(meta_markers)

        # add quals. to be safe
        aggregate = {f"marker_{k}": v for k, v in aggregate.items()}

        result.update(aggregate)

        # some metadata
        result["job_key"] = fc.key
        result["material_code"] = fc.args.get("material")

        def as_df(r, **kwargs):
            return pd.DataFrame([r])

        # apply the function to create the s3 object for the nesting
        fc.apply("style_nest_evaluation", as_df, result, key=fc.key)

    except Exception as ex:
        res.utils.logger.warn(f"failed to generate style nest eval results {repr(ex)}")


def _publish_evaluation(data, key, fc):
    try:

        TOPIC = "res_make.res_nest.nest_evaluations"
        res.utils.logger.info(f"publishing nest evaluation to {TOPIC}")

        kafka = fc.connectors["kafka"]
        num_assets = len(fc.assets)
        data["nested.geometry"] = data["nested"].map(
            lambda g: Polygon(np.stack(g)).exterior
        )

        result = evaluate_nest_v1(data)

        result["job_key"] = key
        result["asset_count"] = num_assets
        result["material"] = fc.args.get("material")
        result["packing_method"] = "heuristic"
        result["created_at"] = res.utils.dates.utc_now_iso_string()

        kafka[TOPIC].publish(
            result,
            coerce=True,
            use_kgateway=True
            # , dedup_on_key="job_key"
        )

        return result
    except Exception as ex:
        res.utils.logger.warn(
            f"Failed to evaluate and publish nest (to the kafka topic) {repr(ex)}"
        )


def nested_outline_dxf(data, fc, **kwargs):
    """
    the nest transforms are read in and we look up the ordered nested outlines for the same keys and center them at the location and write the dxf

    the scaling requires some thought
    - we ready the print compensated outlines
    - we then re-scale by removing the compensation for print and adding the compensation for markers (this is a ratio added above in the marker params)
    - we then recenter the cutlines within the nested geometry so they are located in the correct nest location

    As a check we should see shapes scaled down from what we print so the laser cuter can read in mm

    Fetch what is saved;

        from res.media.images.geometry import *
        from shapely.wkt import loads as shapely_loads
        data = s3.read('s3://res-data-development/flows/v1/dxa-printfile/expv2/nested_outline_dxf/nest-kt-3038-v4-c6533fe637-citrbq-2zzsm/output.feather')['cutlines'].map(shapely_loads)
        ndata = s3.read('s3://res-data-development/flows/v1/dxa-printfile/expv2/nested_outline_dxf/nest-kt-3038-v4-c6533fe637-citrbq-2zzsm/output.feather')['nested.geometry'].map(shapely_loads)

        cascaded_union(list(data)+list(ndata))

    We can generate this nest with something like;

        event = {"apiVersion": "v0", "kind": "Flow", "metadata": {"name": "dxa.printfile", "version": "expv2", "git_hash": "569384b"},
            "args": {"material": "CTNBA", "jobKey": "nest-kt-3038-v4-c6533fe637-citrbq-2zzsm-3", "assets":
                    [{"key": "9999999", "intelligent_one_marker_file_id":"1234" , "asset_id": "recTest",
                        "value": "s3://meta-one-assets-prod/styles/meta-one/3d/kt_3038/v4_c6533fe637/citrbq/2zzsm", "material_code": "CTNBA"}],
                    "addCutline": False}}

        fc = FlowContext(event)
        mps = marker_params_from_event(fc)

    and post to dev say....
    """
    # "nested_outlines"

    from res.media.images.geometry import (
        translate,
        invert_axis,
        LinearRing,
        scale_shape_by,
    )
    from res.media.images.providers.dxf import DxfFile

    # get the marker params
    # MATERIAL_RECOMPENSATION_FACTOR = (
    #     kwargs["MARKER_RECOMP_LENGTH"],
    #     kwargs["MARKER_RECOMP_WIDTH"],
    # )
    MATERIAL_RECOMPENSATION_FACTOR = (
        kwargs["pm_stretch_y"],
        kwargs["pm_stretch_x"],
    )

    res.utils.logger.debug(
        f"Scaling parameteres from material props {MATERIAL_RECOMPENSATION_FACTOR}"
    )

    def scale_xy(g):
        """
        re-compensate paper marker dims
        """
        g = geom_scale(
            g,
            yfact=MATERIAL_RECOMPENSATION_FACTOR[0],
            xfact=MATERIAL_RECOMPENSATION_FACTOR[1],
        )

        # nest -fit them in pixels
        # nested outlines are already scaled to pixel space in the meat one
        return g  # scale_shape_by(1/DPMM)(g)

    image_keys = kwargs.get("image_keys")
    job_key = kwargs.get("job_key")
    res.utils.logger.info(f"Preparing the DXF file for the job key {job_key}")

    items = fc.get_node_data("notched_outlines", keys=image_keys)

    items = [invert_axis(swap_points(LinearRing(d.values))) for d in items]

    data["cutlines"] = items

    def move_it(row):
        """
        center the cutlines in the geometry
        """
        g = row["nested.geometry"]
        c1 = g.centroid
        h = row["cutlines"]
        c2 = h.centroid
        return translate(h, c1.x - c2.x, c1.y - c2.y)

    res.utils.logger.info(f"scaling cutlines for mms and paper marker compensation")

    # these are compensated in mms but we should scale them into the nest so we can scale down again
    data["cutlines"] = data["cutlines"].map(scale_xy)

    data["cutlines"] = data.apply(move_it, axis=1)

    path = fc.get_node_path_for_key("nested_outline_dxf", key=fc.key)
    path = f"{path}/cutfile.dxf"

    res.utils.logger.info(
        f"Exporting a cutline dxf file to the path {path} - scaling to mm from pixels"
    )

    # export the dataframe cutlines and scale the shapes
    DxfFile.export_dataframe(
        data,
        path,
        column="cutlines",
        # the marker returns notched outlines in the pixel space
        # we can fit them into the nests and scale the entire thing down to mms
        point_conversion_op=scale_shape_by(1 / DPMM),
    )

    data["cutlines"] = data["cutlines"].map(str)

    return data


def _get_node_data_for_assets_in_env(assets, node, env_bucket="res-data-production"):
    """

    Some convention hard coding to get data for parts or alpha hulls
    the node could be any node in print file such as extract parts or alpha hull node
    the environment is defaulting to prod as we have test data there but could be res-data-development or res-data-production too

    When we save print pieces today we can manage legacy or new meta-one "markers"
    we can also test healing for both cases
    In future we will store the data in a database and just point to paths on S3

    """
    VERSION = "v1"
    RES_DATA_BUCKET = env_bucket  # or -development or -platform
    FLOW_CACHE_PATH = f"s3://{RES_DATA_BUCKET}/flows/{VERSION}"

    s3 = res.connectors.load("s3")
    root = f"{FLOW_CACHE_PATH}/dxa-printfile/expv2/{node}"  # OR alpha_hulls

    keys = [_determine_image_key_from_path(asset["value"], asset) for asset in assets]

    for key in keys:
        path = f"{root}/{key}"
        files = list(s3.ls(path))
        res.utils.logger.debug("*****list files in", path)
        for f in files:
            res.utils.logger.debug(f"Loading {f}")

            yield s3.read(f)


def perform_nesting(event, context=None):
    args = parse_args(event, context)
    material = args.get("material")
    job_key = args.get("job_key")
    nest_retries = args.get("nest_retries")
    buffer = args.get("buffer")
    output_bounds_length = args.get("output_bounds_length")
    shrink_output_if_possible = args.get("shrink_output_if_possible")
    write_file = args.get("write_file")
    image_keys = args.get("image_keys")
    header_image = args.get("header_image")

    res.utils.logger.incr("nesting_printfile_started")

    try:
        res.utils.logger.info(
            "NEW: Saving nest payloads to snowflake after splitting pieces as we generate the nesting"
        )
        df = pd.DataFrame([event["args"]])
        augment_nested_assets(df, save=True)
    except Exception as ex:
        res.utils.logger.warn(f"Failing to augment and save nest payload {repr(ex)}")

    assert (
        material is not None
    ), f"someone is making an illegal attempt to nest without a material (code): {job_key}"

    with FlowContext(
        event, context, node="nesting", data_group=args.get("material")
    ) as fc:
        # these are actually not the ones that map to OUR params. refactor
        marker_params = _try_get_material_comp(fc, material)
        # very very subtle and very very sad: these lookups only work on regular UUID keys and now we are adding something for a new type of helaing key UUID/n
        lookup_ones = {
            _determine_image_key_from_path(p["value"], p).split("/")[0]: p["key"]
            for p in fc.assets
        }

        event_args = event.get("args", {})
        if "compensation_x" in event_args:
            custom_x = event_args.get("compensation_x")
            res.utils.logger.debug(f"Using a compensation value for x {custom_x}")
            marker_params["stretch_x"] = custom_x
        if "compensation_y" in event_args:
            custom_y = event_args.get("compensation_y")
            res.utils.logger.debug(f"Using a compensation value for y {custom_y}")
            marker_params["stretch_y"] = custom_y

        MA = event.get("args").get("max_assets", MAX_ASSETS)
        if MA != -1 and len(image_keys) > MA:
            res.utils.logger.info(f"restricting number of assets to max assets={MA}")
            image_keys = image_keys[:MA]

        if event.get("args", {}).get("spoof_fail", False) == True:
            raise Exception("spoofing fail to test argo flow")

        # count the hulls or the pieces extracted and
        image_count = fc.get_node_data_count("extract_alpha_hulls", keys=image_keys)
        if image_count == 0 or image_count > MAX_ALLOWED_PIECES:
            e_message = f"There must be between 0 and {MAX_ALLOWED_PIECES} pieces - failing the job!"
            res.utils.logger.warn(
                f"Image keys produced an invalid number of pieces. Keys: {image_keys}"
            )
            res.utils.logger.warn(e_message)
            raise Exception(e_message)

        done = False
        tries = nest_retries
        nest_transforms = None
        res.utils.logger.debug(f"Nesting initially with keys {image_keys}")

        while not done and tries > 0:
            # load the coordinate dataframe (TODO: think more carefully about how we deal with formats)
            try:
                # trim one from the end and try again

                all_bounds = [
                    # we are going to pass XY format to nesting algo
                    data[["column_1", "column_0"]].values
                    for data in fc.get_node_data("extract_alpha_hulls", keys=image_keys)
                ]

                files = fc.get_node_file_names("extract_alpha_hulls", keys=image_keys)

                # how we resolve the ONE from the file name
                one_keys = [f.split("/")[-2] for f in files]
                one_keys = [lookup_ones.get(k) for k in one_keys]

                res.utils.logger.info(f"resolved ones from keys {one_keys}")

                nest_transforms = fc.apply(
                    node="perform_nest_compute",
                    func=optimization.nest.nest_transforms,
                    data=all_bounds,
                    key=job_key,
                    **marker_params,
                    buffer=buffer,
                    output_bounds_length=output_bounds_length,
                    one_number=one_keys,
                )
                done = True

            except ValueError as vex:
                # in this case we should log it as a problem asset and die
                res.utils.logger.debug("Exiting after hitting a value error in nesting")
                raise vex
            except Exception as ex:
                res.utils.logger.info(
                    f"Failed to nest {len(image_keys)} - removing one (reason: {repr(ex)})"
                )
                image_keys = image_keys[:-1]
                tries -= 1

        assert (
            nest_transforms is not None
        ), "It has not been possible to find a valid nesting solution for the assets and bounds requested"

        # try store nest-eval node data - might want to use parquet for this

        result = _publish_evaluation(nest_transforms, job_key, fc)

        # expected_image_count = fc.get_node_data_count("extract_alpha_hulls", keys=image_keys)
        # we can check this against the nesting and then raise an error in the nesting apply if we dont have enough pieces

        # pass the attempted possibly pruned set of keys we wanted to nest
        solution = _evaluate_nesting(
            event,
            nest_transforms,
            try_image_keys=image_keys,
            requested_pieces=all_bounds,
        )

        solution_res = None

        # will probably turn this block off
        try:
            # send an event saying we nested but we may not have made the file
            solution_res = on_nesting_solutions_notify(
                event, context, solution=solution
            )
        except Exception as ex:
            res.utils.logger.warn(
                f"ERROR: Unable to notify print request due to {repr(ex)}. Will try to make the print file anyway"
            )

        if shrink_output_if_possible:
            # if the locations are using more space than they need translate them its unfortunate that the nesting algorithm is indexed from high ys
            # it should really be adapter to nest from the origin and then we could more elegantly clip the image.
            # as we invert the fill on print file, we can just use new_heigh - Y instead of requestHeight-y
            nested_length = solution["evaluation"]["nested_length"]
            output_bounds_length = nested_length + 10  # arbitrary buffer
            res.utils.logger.debug(
                f"Forcing printfile max size to {output_bounds_length} because this is all we needed in nesting"
            )

        if write_file:
            # TODO: only when we are not doing cutlines we run the node from here LEARNING
            image_source_node = "extract_parts"  # "draw_part_outlines"

            # TODO:: - count node data and raise exception if not between 0 and some reasonable number
            s3 = res.connectors.load("s3")
            image_paths = [
                path
                for image_key in image_keys
                for path in s3.ls(
                    fc.get_node_path_for_key(image_source_node, image_key)
                )
            ]

            # image_generator = list(image_generator)
            # if len(image_generator) == 0:
            #     message = f"Quiting an illegal print request for job {job_key}. There are 0 image pieces to composite"
            #     res.utils.logger.error(measure)
            #     raise FlowException(message=message)

            tag_locations = []
            tag_icons = []  # this will be from config?
            outpath = fc.get_node_path_for_key("perform_nest_apply", key=job_key)
            result = fc.apply(
                node="perform_nest_apply",
                func=optimization.nest.nest_transformations_apply,
                data=nest_transforms,
                image_paths=image_paths,
                output_path=outpath,
                tag_locations=tag_locations,  # this can actually be transformatins like a rotation too?
                tag_icons=tag_icons,
                key=job_key,
                **marker_params,
                buffer=buffer,
                output_bounds_length=output_bounds_length,
                header_image=header_image,
                header_data=get_nest_key_from_argo_job_key(job_key),
            )

            # genreate the dxf from the nested transform merged with the notched outlines...

            # borken abstraction just because we are treating the dataframe as the output and side effect on the DXF
            try:
                fc.apply(
                    node="nested_outline_dxf",
                    func=nested_outline_dxf,
                    key=job_key,
                    data=nest_transforms,
                    image_keys=image_keys,
                    job_key=job_key,
                    fc=fc,
                    **marker_params,
                )

                # post the nesting attachment
            except Exception as ex:
                # because this is an experimental feature we are in best effort mode
                res.utils.logger.warn(f"FAILED TO CREATE CUT FILE")

            solution_res = on_nesting_solutions_notify(
                event,
                context,
                solution=solution,
                file_ready=True,
            )
        else:
            res.utils.logger.debug("skipping file write as option set to False")

        res.utils.logger.incr("nesting_printfile_started")

        return solution_res  # nest_transforms


def on_task_fail(event, context):
    args = parse_args(event, context)
    image_keys = args.get("image_keys")
    # error details are better than errors
    errors = event.get("errors_details", [])
    material = (
        args.get("material") or args.get("material_code") or event.get("material_code")
    )
    # TODO: how to add context for the failure to sentry
    # the message needs to be groupable but the details for drill down need to have the payload and other splitters

    node = event.get("metadata", {}).get("node")

    res.utils.logger.debug(f"Processing task failure with event {event}")

    # here we do not enter the flow context because that would count as an invocation etc.
    # rather we just use it to parse
    fc = FlowContext(event, context, node=node, data_group=material)

    if "OOM" in str(errors):
        fc.log_errors(
            "res-nest failure due to OOM or other error not handled in code",
            errors=errors,
        )
    # increment metrics here
    fc.metric_incr(verb="failed")

    all_bounds = [
        # we are going to pass XY format to nesting algo
        data[["column_1", "column_0"]].values
        for data in fc.get_node_data("extract_alpha_hulls", keys=image_keys)
    ]

    solution = _evaluate_nesting(
        event,
        None,
        try_image_keys=image_keys,
        requested_pieces=all_bounds,
    )

    # try send error to handler
    try:
        # send an event saying we nested but we may not have made the file
        on_nesting_solutions_notify(
            event, context, solution=solution, status_code=500, errors=errors
        )
    except Exception as ex:
        print(
            f"ERROR: Unable to notify print request due to {repr(ex)}. Will try to make the print file anyway"
        )


def _write_solution_metrics(solution):
    evaluation = solution.get("evaluation", {})
    for k, v in evaluation.items():
        res.utils.logger.incr(f"nesting_printfile_{k}", v)


def scrape_nest_notify(event, context):
    import res
    import pandas as pd

    s3 = res.connectors.load("s3")
    from ast import literal_eval
    from tqdm import tqdm
    from res.utils.dates import relative_to_now

    environment = "production"
    key = "ctw70-316d441fbb-2021-09-18-08-02-05-reproduce"

    all_data = []
    for f in tqdm(
        s3.ls_info(
            "s3://res-data-production/flows/v1/dxa-printfile/expv2/.meta/",
            modified_after=relative_to_now(14),
        )
    ):
        key = f["path"].split("/")[-2]
        # print('loading', key)
        lines = [
            line
            for f in s3.ls(f"s3://argo-{environment}-artifacts/{key}")
            for line in s3.read_text_lines(f)
        ]
        for l in lines:
            if "Notify nesting solution" in l:
                result = []
                timestamp = l[: l.index(" [")]
                start = l.index("{")
                data = literal_eval(l[start:].replace("\n", ""))
                for a in data.get("nested_assets"):
                    a["status"] = True
                    a["job"] = key
                    a["timestamp"] = timestamp
                    result.append(a)
                for a in data.get("omitted_assets"):
                    a["status"] = False
                    a["job"] = key
                    a["timestamp"] = timestamp
                    result.append(a)
                break

        all_data.append(pd.DataFrame(result))

    all_data = pd.concat(all_data).to_csv(
        "s3://res-data-platform/samples/data/nest_jobs.csv"
    )


def on_nesting_solutions_notify(
    event, context, solution, file_ready=False, status_code=200, errors=[]
):
    res.utils.logger.info(f"Notify nesting solution {solution}")
    solution["print_file_ready"] = file_ready
    solution["status_code"] = status_code
    solution["errors"] = errors
    solution["user_data"] = event.get("args", {}).get("user_data")

    # unless we know which child failed we cannot do this
    # for e in error:
    #     if "OOMKilled" in e:
    #         assets = event['args']['assets']
    #         if len(assets) == 1:
    #             res.utils.logger.debug

    _write_solution_metrics(solution)

    with FlowContext(event, context) as fc:
        for asset in solution["nested_assets"]:
            fc.publish_asset_status(asset, "nesting", "", "DONE")

    callback = event.get("args", {}).get("on_nesting_solution_notify", None)
    if callback:
        if "http" in callback:
            res.utils.safe_http.request_post(callback, data=solution)
        else:
            res.utils.post_to_lambda(callback, solution)

    return solution


def flow_performance(data):
    """
    Every flow should implement an evaluation node that is custom to itself
    For example in the printflow we can talk about CPU costs, space utilization factors etc.
    """
    return []


def get_loglines(event, context, job_key=None):
    with FlowContext(event, context) as fc:
        job_key = job_key or event.get("args").get("jobKey")
        s3 = fc.connectors["s3"]
        l = []
        for f in s3.ls(f"s3://argo-dev-artifacts/{job_key}/"):
            for line in s3.read_text_lines(f):
                l.append(line)
        return l


class ResNestV1:
    """
    Utility to retrieve data from s3 and render it if it has been generated by this flow
    """

    def __init__(self, data):
        s3 = res.connectors.load("s3")
        self._path = None
        self._event = None
        self._fc = None
        self._assets = None
        self._shape = None

        if isinstance(data, str):
            self._path = data

            if s3.exists(data):
                data = s3.read(data)
            else:
                data = pd.DataFrame()

            self._event = ResNestV1.get_nested_event(self._path)
            self._fc = FlowContext(self._event)

        try:
            if len(data):
                data["geometry"] = data["nested"].map(
                    lambda g: Polygon(np.stack(g)).exterior
                )
                # add the alias
                data["nested.geometry"] = data["geometry"]
            self._assets = pd.DataFrame(self._event["args"]["assets"])
            self._assets["marker_filekey"] = self._assets["value"].map(
                lambda x: x.split("/")[-1].split(".")[0]
            )
            self._assets = self._assets.rename(columns={"key": "one"})
        except Exception as ex:
            print("warning - failed to make a geometry", repr(ex))
        self._df = data
        if len(data):
            self._shape = cascaded_union(data["geometry"].values)

    def reproduce_prod_on_dev(self, add_hash=True):
        """
        simple util to reproduce a production job on dev using the given key
        """
        from res.utils.safe_http import request_post

        event = self._event

        tail = "-reproduce"
        if add_hash:
            tail += f"-{res.utils.res_hash()}"
        event["args"]["jobKey"] = event["args"]["jobKey"] + tail.lower()

        r = request_post(
            "https://datadev.resmagic.io/res-connect/res-nest",
            json={
                "event": {
                    k: v
                    for k, v in event.items()
                    if k not in ["errors", "error_details"]
                }
            },
        )
        res.utils.logger.info(f'Nest reproduction as {event["args"]["jobKey"]}')
        return r.json()

    def validate_any_meta_ones(self):
        """
        simply helper
        loop assets for something that looks like a meta one
        load it and call its validate

        the return maps the meta one path to and list of validation errors
        """
        from res.flows.meta.marker import MetaMarker

        df = self._assets
        df["is_meta_one"] = df["value"].map(lambda x: "meta-one" in x)
        mos = list(df[df["is_meta_one"]]["value"].unique())

        return {mo: MetaMarker(mo).validate() for mo in mos}

    def get_nest_evaluation(self):
        """
        Nest evaluation statistics
        Read the nest data and evaluate it with some metadata attribute
        """
        data = self._df

        result = evaluate_nest_v1(data)

        result["job_key"] = self._fc.key
        result["asset_count"] = len(self._assets)
        result["material"] = self._fc.args.get("material")
        result["created_at"] = res.utils.dates.utc_now_iso_string()

        return result

    @property
    def flow_context(self):
        return self._fc

    @property
    def image_keys(self):
        return [
            _determine_image_key_from_path(p["value"], p, method="stem")
            for p in self._event["args"]["assets"]
        ]

    @property
    def ones(self):
        return list(self._assets["one"])

    def get_one_marker_image(self, one):
        from PIL import Image

        s3 = res.connectors.load("s3")
        recs = self._assets.query(f'one=="{one}"')
        assert len(recs), f"There are no records matching this one number: {one}"

        mpath = recs.iloc[0]["value"]

        res.utils.logger.warn(
            f"reading image {mpath} [TODO check it exists and is an image]"
        )

        return Image.fromarray(s3.read(mpath))

    def enumerate_extracted_parts(self, one):
        from PIL import Image

        s3 = res.connectors.load("s3")
        path = self._path_for_one("extract_parts", one)
        for f in s3.ls(path):
            res.utils.logger.info(f"reading {f}")
            yield Image.fromarray(s3.read(f))

    def _path_for_one(self, node, one):
        recs = self._assets.query(f'one=="{one}"')
        assert len(recs), f"There are no records matching this one number: {one}"
        key = recs.iloc[0]["marker_filekey"]
        path = "/".join(self._path.split("/")[:-2]).replace("perform_nest_apply", node)
        return f"{path}/{key}"

    def get_logs(self):
        s3 = res.connectors.load("s3")
        env = "production" if "res-data-production" in self._path else "dev"
        files = list(s3.ls_info(f"s3://argo-{env}-artifacts/{self._fc.key}"))
        logs = [list(s3.read_text_lines(f["path"])) for f in files]

        return logs

    def get_marker_mask(self, one):
        s3 = res.connectors.load("s3")
        path = self._path_for_one("masks", one)
        path = f"{path}/mask.png"
        return s3.read(path)

    def get_marker_outlines(self, one):
        s3 = res.connectors.load("s3")
        path = self._path_for_one("masks", one)
        path = f"{path}/piece_outlines.feather"
        return s3.read(path)

    def show_cutlines_and_nests(self):
        # mino9r change
        # TODO - get the path of the cutfile node
        # example cf = 's3://res-data-development/flows/v1/dxa-printfile/expv2/nested_outline_dxf/nest-kt-3038-v4-c6533fe637-citrbq-2zzsm/cutfile.dxf'
        # dxf = s3.read(cf)
        # from res.media.images.geometry import *
        # from shapely.wkt import loads as shapely_loads
        # cuts = cascaded_union(dxf.compact_layers['geometry'])

        # nf = s3.read('s3://res-data-development/flows/v1/dxa-printfile/expv2/nested_outline_dxf/nest-kt-3038-v4-c6533fe637-citrbq-2zzsm/output.feather')
        # nf['nested.geometry'] = nf['nested.geometry'].map(shapely_loads)
        # nests = cascaded_union(nf['nested.geometry'])
        # cascaded_union([cuts,nests])
        pass

    def get_nest_evaluation(self):
        sample = evaluate_nest_v1(self.data)
        sample["job_key"] = self._fc.key
        sample["material"] = self._fc.args["material"]
        sample["asset_count"] = len(self._fc.assets)
        return sample

    def get_one_piece_files(self):
        """
        Using the assets in the payload we can match the file ids to ONEs
        We must trust the sort order of the file names though
        """
        df = pd.DataFrame(
            self._fc.get_node_file_names("extract_parts", keys=self.image_keys),
            columns=["path"],
        )
        df["key"] = df["path"].map(lambda x: x.split("/")[-2])
        df = pd.merge(
            df,
            self._assets[["marker_filekey", "one", "material_code"]],
            left_on="key",
            right_on="marker_filekey",
        )
        return df.drop("marker_filekey", 1)

    @property
    def key(self):
        return self._fc.key

    @property
    def data(self):
        return self._df

    @property
    def shape(self):
        return self._shape

    @property
    def num_pieces(self):
        return len(self._df)

    @staticmethod
    def get_nested_event(pth):
        s3 = res.connectors.load("s3")
        key = pth.split("/")[-2]
        root = "/".join(pth.split("/")[:-3])
        meta = f"{root}/.meta/{key}/payload.json"
        event = s3.read(meta)
        return event

    @staticmethod
    def from_production_key(key):

        path = f"s3://res-data-production/flows/v1/dxa-printfile/expv2/perform_nest_apply/{key}/output.feather"

        return ResNestV1(path)

    @staticmethod
    def get_nested_outlines_for_job_path(pth, return_paths=False):

        s3 = res.connectors.load("s3")
        res.utils.logger.info(f"loading nests data using path {pth}")
        event = ResNestV1.get_nested_event(pth)
        root = pth[: pth.index("perform_nest_compute")]
        markers = [f["value"] for f in event["args"]["assets"]]
        marker_keys = [f.split("/")[-1].split(".")[0] for f in markers]

        paths = [f"{root}extract_alpha_hulls/{k}" for k in marker_keys]
        paths = list(itertools.chain(*[list(s3.ls(p)) for p in paths]))

        if return_paths:
            return paths

        # return pd.DataFrame([s3.read(p).astype(object).values for p in paths],columns=['geometry'])
        return pd.DataFrame(
            [Polygon(s3.read(p).values) for p in paths], columns=["geometry"]
        )

    def _repr_svg_(self):
        return self.shape._repr_svg_() if self._shape else None


def renest_roll_v1(df, max_width=25000, max_length=2000000, plot=False):
    """
    we carefully renest pieces that were nested
    because the nested shapes are actually the outlines we nested with all buffers and comps applied we should renest EXACTLY these shapes
    We now the compensation but we assume already compd etc
    if we were reloading the hulls for example then we would still need to add nesting buffers between pieces!!!

    the input to this data has a contract with material props and the original nests (chunks)
    """
    from res.learn.optimization.nest import (
        nest_dataframe_by_chunks,
    )

    roll_data = pd.concat(ResNestV1(f)._df for f in df["nest_data"]).reset_index()
    roll_data["res.key"] = None
    # the row should have material props: contract
    sample = dict(df.iloc[0])
    stretch_x = sample["Compensation X"]
    stretch_y = sample["Compensation Y"]
    # make this required in future
    width = sample.get("material_width", max_width)
    roll_data = nest_dataframe_by_chunks(
        roll_data,
        output_bounds_length=max_length,
        output_bounds_width=width,
        stretch_x=1.0,  # stretch_x,
        stretch_y=1.0,  # stretch_y,
        plot=plot,
        # do not buffer the pieces as they are already buffered
        buffer=0,
        # now we explicitly do not reverse points as we trust the nestv1 source for this order
        reverse_points=False,
    )

    return roll_data


def build_pack_piece_payload_from_completed_printfile_job(
    job_key, task_key, **job_args
):
    """
    Utility method to create a payload to the pack_pieces flow based on a printfile
    nesting job which has already completed (i.e., the pieces are all extracted etc).
    The point of this is just so we can run the pack_pieces flow in parallel with the old
    nesting until we get to a point where we want to cut over to it.
    For now this has to live in printfile since it depends on so many parts of it.
    """
    nest = ResNestV1.from_production_key(job_key)
    return build_pack_piece_payload_from_completed_printfile_flow_context(
        nest._fc, job_key, task_key, **job_args
    )


def build_pack_piece_payload_from_completed_printfile_flow_context(
    flow_context, job_key, task_key, **job_args
):
    s3 = res.connectors.load("s3")
    nest_args = flow_context._event["args"]
    nest_assets = nest_args["assets"]
    image_keys = [
        _determine_image_key_from_path(p["value"], p, method="stem")
        for p in nest_assets
    ]
    input_assets = []
    for k, asset in zip(image_keys, nest_assets):
        keys_pieces = list(
            s3.ls(flow_context.get_node_path_for_key("extract_alpha_hulls", k))
        )
        keys_images = list(
            s3.ls(flow_context.get_node_path_for_key("extract_parts", k))
        )
        if len(keys_pieces) != len(keys_images):
            raise Exception(
                f"Failed to build payload: found {len(keys_pieces)} pieces and {len(keys_images)} images for key {k}"
            )
        for piece, image in zip(keys_pieces, keys_images):
            input_assets.append(
                {
                    "s3_shape_path": piece,
                    "s3_image_path": image,
                    "asset_id": asset["asset_id"],
                    "asset_key": asset["key"],
                    "dxf_outline": None,
                }
            )
    material_width = _try_get_material_comp(flow_context, nest_args["material"])[
        "output_bounds_width"
    ]
    combined_job_args = {
        "stretch_x": nest_args["compensation_x"],
        "stretch_y": nest_args["compensation_y"],
        "output_bounds_width": int(material_width),
        "output_bounds_length": nest_args["output_bounds_length_inches"] * DPI_SETTING,
        "buffer": nest_args["buffer"],
        "material": nest_args["material"],
        "on_nesting_solution_notify": nest_args["on_nesting_solution_notify"],
        "job_key": job_key,  # re-use the same job key so that we have parallel structure under the new flow.
        **job_args,
    }
    return {
        "apiVersion": "resmagic.io/v1",
        "kind": "resFlow",
        "metadata": {"name": "make.nest.pack_pieces", "version": "experimental"},
        "assets": input_assets,
        "args": combined_job_args,
        "task": {"key": task_key},
    }
