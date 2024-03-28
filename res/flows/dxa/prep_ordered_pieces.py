"""
Prep ordered pieces is the piece intelligent Make-ONE production or Make.Meta.One
So we get digital assets and other assets and make sure they are accessible to folks

TODO: 
 - take order priority scores e.g. functions of brand success on order items 
"""

import res
import cv2
import os
import re
import numpy as np
import pandas as pd
from pathlib import Path
from pyairtable import Table
from res.flows.meta.marker import MetaMarker
from res.flows import FlowContext, FlowException, flow_node_attributes
from res.flows.make.nest.utils import flag_asset, unflag_asset
from res.flows.make.nest.progressive.utils import add_contract_variables_to_assets
from res.media.images.geometry import swap_points
from res.media.images.outlines import enumerate_pieces, get_piece_outline
from res.learn.optimization.nest import nest, pixels_to_yards, plot_comparison
from res.media import images
from res.media.images.geometry import (
    LinearRing,
    Polygon,
    shift_geometry_to_origin,
    is_bias_piece,
)
from res.flows.FlowContext import AssetFlaggedFlowException, NamedFlowAsset
from res.connectors.s3.datalake import list_astms_2d
from res.utils import secrets_client, ping_slack
from shapely.ops import unary_union
from shapely.affinity import rotate
from res.flows.make.production.inbox import _ppp_move_pieces_to_roll_packing_enter
import traceback
from res.flows.make.production.queries import (
    get_one_number_style_sku_order_rank,
    get_style_code_ranks_for_style_codes,
)
from res.flows.make.production.prepare_assets import (
    handler as make_handler,
    cache_handler,
)

# the prod path for pieces is something like this "s3://res-data-production/flows/v1/dxa-prep_ordered_pieces/primary/extract_parts/<KEY>"

RES_ENV = os.environ.get("RES_ENV")
REQUEST_TOPIC = "res_meta.dxa.prep_pieces_requests"
RESPONSE_TOPIC = "res_meta.dxa.prep_pieces_responses"
DPI = 300
DEFAULT_NEST_BUFFER = 75
PIECE_IMAGE_SLOP = 100  # how far outside the polygon we are willing to let the image spill - in pixels. -- allowing tons of slop for now.
ASSET_PAYLOAD = {
    "one_number": None,
    "body_code": None,
    "material_code": None,
    "color_code": None,
    "size_code": None,
    "piece_type": None,
    "path": None,
    "metadata": {
        "intelligent_marker_id": None,
        "max_image_area_pixels": None,
        # **material_props and buffer  = None
    },
}


class _dummy_iom_piece:
    def __init__(self):
        pass

    def __getitem__(self, key):
        return None

    def get(self, key, default=None, **kwargs):
        return None


def fix_sku(s):
    if s[2] != "-":
        return f"{s[:2]}-{s[2:]}"
    return s


def make_style_code(sku):
    sku = f" ".join(sku.split(" ")[:3])
    return fix_sku(sku)


class IntelligentOneMarker:
    """
    wraps access to the intelligent one marker, primarily using the path to the image
    but we could also look up the file id

    we use this class just as an abstraction - it is necessary to call the image fetch to make
    the outlines available and this is done just to save resources and for this use case
    """

    @staticmethod
    def get_marker_path(imid):
        mongo = res.connectors.load("mongo")
        data = pd.DataFrame(
            mongo["resmagic"]["files"].find({"_id": mongo.object_id(imid)})
        )
        data = data.iloc[0]["s3"]

        return f"s3://{data['bucket']}/{data['key']}"

    @staticmethod
    def from_intelligent_marker_file_id(fid, **kwargs):
        # find a path from fid - this is a mongo thing
        path = IntelligentOneMarker.get_marker_path(fid)
        return IntelligentOneMarker(path, **kwargs)

    def try_get_setup_data_for_body_version(self):
        """
        if we have processed this along with a raster, we would know the ordinals and we can use that for the names
        def get_net_file(body, size, version=None, combo=False):

           ->  's3://meta-one-assets-prod/bodies/net_files/jr_3055/v4/jr_3055_v4_net_2x_15x15.dxf',

        The net file would have been used to get the names to match the setup file that was rasterized and then we have the ordinals for the intelligent marker

        """

        from res.media.images.providers.dxf import DxfFile

        def _make_key(body_code, body_version, size):
            v = str(body_version).lower().replace("v", "")
            return f"{body_code}-V{v}-{size}".upper()

        def get_body(body_code, body_version, size):
            db = res.connectors.load("dynamo")["production"]["body_net_dxfs"]
            k = _make_key(body_code, body_version, size)
            data = db.get(k)
            res.utils.logger.debug(f"key lookup {k}")
            if data is not None and len(data):
                return dict(data.iloc[0])["files"]
            return {}

        data = get_body(self._body, self._body_version, self._size)
        if data:
            if self._rank == "Combo":
                f = data.get("combo")
                _, df = DxfFile.make_setup_file_svg(f, return_shapes=True)
            else:
                f = data.get("self")
                _, df = DxfFile.make_setup_file_svg(f, return_shapes=True)
            return df
        # #from res.flows.dxa.setup_files import

        # # return None
        # rank = None
        # return get_net_file(
        #     self._body,
        #     body_version=self._body_version,
        #     size=self._size,
        #     combo=rank == "Combo",
        # )

    def try_get_dxf_for_body_version(self):
        """
        uses path convention to find the dxf for the body and version
        fails with warning and returns none if cannot resolve

        dxf = try_get_dxf_for_body_version('TK-6079')

        dxf = try_get_dxf_for_body_version('TK-6079',version=8)

        """

        try:
            res.utils.logger.debug(f"Finding body dxf {self._body}")
            result = list_astms_2d(self._body)
            res.utils.logger.debug(f"found {len(result)} for {self._body}")
            version = self._body_version
            if version is not None:
                res.utils.logger.debug(f"searching for version {version}")
                a = result[result["meta_version"].map(str) == str(version)]
                if len(a):
                    result = a
            return res.connectors.load("s3").read(
                result.iloc[0]["path"], assume_no_suffix_is_self=True
            )
        except Exception as ex:
            res.utils.logger.warn(
                f"Unable to load dxf for body/version {res.utils.ex_repr(ex)}"
            )
            return None

    def __init__(self, path, **asset):
        self._path = path
        self._key = Path(self._path).stem
        self._material_code = asset.get("material_code")
        self._size = asset.get("size_code")
        self._body = asset.get("body_code")
        self._body_version = asset.get("body_version")

        # the material props are contained in the metadata along with other things although these are generated over the payload in the generator below and not in the raw kafka message
        self._material_props = asset.get("metadata", {})
        self._rank = self._material_props.get("rank")
        self._piece_outlines = []

    def __getitem__(self, key, **kwargs):
        """
        implementing marker interface
        """
        return _dummy_iom_piece()

    @property
    def invalidated_date(self):
        return None

    @property
    def is_valid(self):
        return True

    @property
    def key(self):
        """
        The key was taken from the path
        For example markers like `s3://resmagic/uploads/b107d994-187a-4963-8aa6-7deb780447f6.png`
        have a key `b107d994-187a-4963-8aa6-7deb780447f6`
        """
        return self._key

    def get_nesting_statistics(self, plot=False):
        """
        Having called the self iterator to stream the pieces and load the outlines
        we can nest the outlines and get the statistics
        """
        from res.learn.optimization.nest import evaluate_nest_v1

        def with_tag(df, col):
            """
            for renaming purposes clone the dataframe with tagged geom names for different contexts
            """
            df = df.copy().rename(
                columns={"geometry": col, f"nested.geometry": f"nested.{col}"}
            )
            df["geometry_column"] = col
            return df

        def hull_of(l):
            return [g.convex_hull.exterior for g in l]

        def remove_physical(l):
            """
            using self material props, remove the buffer (which we need to add as attr and also the comp
            Buffering a linearring produces an empty geom so we make a polygon, then shift to origin for compat with image space contract and the take the new smaller boundary

            We also want to make sure we have the completed polygon boundary exterior
            """
            BUFFER = self._material_props.get("offset_size")
            if not BUFFER or pd.isnull(BUFFER):
                return list(l)

            # image scaler...
            BUFFER = -300 * BUFFER

            return [
                shift_geometry_to_origin((Polygon(g)).buffer(BUFFER).boundary)
                for g in l
            ]

        # load the images
        if not self._piece_outlines:
            res.utils.logger.info("preloading images")
            self.load_pieces()

        alldfs = []

        n = pd.DataFrame(
            [
                {"nested": o}
                for o in nest(
                    hull_of(self.piece_outlines),
                    **{k: v for k, v in self._material_props.items()},
                )
            ],
            dtype="object",
        )

        n["nested.geometry"] = n["nested"].map(lambda g: LinearRing(np.stack(g)))

        physical_nest = evaluate_nest_v1(n, should_update_shape_props=True)
        alldfs.append(with_tag(n, "physical_geometry"))

        # now do the same thing but remove the buffer and allow it to be compensated

        raw_outlines = remove_physical(self.piece_outlines)

        n = pd.DataFrame(
            [
                {"nested": o}
                for o in nest(
                    hull_of(raw_outlines),
                    **{k: v for k, v in self._material_props.items()},
                )
            ],
            dtype="object",
        )

        n["nested.geometry"] = n["nested"].map(lambda g: LinearRing(np.stack(g)))
        compensated_nest = evaluate_nest_v1(n, should_update_shape_props=True)
        alldfs.append(with_tag(n, "compensated_geometry"))
        # now do the same thing for the comp comp and buffer

        # now again use the RAW outlines but this time DO NOT compensate either by overriding props to 1
        new_props = dict(self._material_props)
        new_props["stretch_x"] = 1
        new_props["stretch_y"] = 1

        n = pd.DataFrame(
            [
                {"nested": o}
                for o in nest(
                    hull_of(raw_outlines),
                    **{k: v for k, v in new_props.items()},
                )
            ],
            dtype="object",
        )

        n["nested.geometry"] = n["nested"].map(lambda g: LinearRing(np.stack(g)))
        raw_nest = evaluate_nest_v1(n, should_update_shape_props=True)
        alldfs.append(with_tag(n, "raw_geometry"))

        if plot:
            alldfs = pd.concat(alldfs).reset_index()
            alldfs["key"] = alldfs["index"]
            plot_comparison(alldfs)

        piece_count = raw_nest["piece_count"]

        raw_nest = {
            f"{k}_raw": v for k, v in raw_nest.items() if k not in ["piece_count"]
        }

        raw_nest["piece_count"] = piece_count

        physical_nest = {
            f"{k}_physical": v
            for k, v in physical_nest.items()
            if k not in ["piece_count"]
        }
        compensated_nest = {
            f"{k}_compensated": v
            for k, v in compensated_nest.items()
            if k not in ["piece_count"]
        }

        physical_nest.update(raw_nest)

        physical_nest.update(compensated_nest)

        return physical_nest

    def try_buffer(self, named_piece_outlines):
        offset_size_inches = self._material_props.get("offset_size")
        if pd.notnull(offset_size_inches):
            offset = offset_size_inches * 300
            res.utils.logger.debug(
                f"Buffering by offset size inches {offset_size_inches}"
            )
            named_piece_outlines = {
                k: Polygon(g).buffer(offset).exterior
                for k, g in named_piece_outlines.items()
            }
        return named_piece_outlines

    def __iter__(self):
        s3 = res.connectors.load("s3")
        # this is only created here - flawed but
        self._names = []
        named_outlines = None

        try:
            net_file = self.try_get_setup_data_for_body_version()

            if net_file is not None:
                named_outlines = dict(net_file[["key", "geometry"]].values)
                named_outlines = {k: v for k, v in named_outlines.items()}
                named_outlines = self.try_buffer(named_outlines)

            else:
                # if we can use the DXF file its like this
                dxf = self.try_get_dxf_for_body_version()
                if dxf:
                    s = dxf.size_from_accounting_size(self._size)
                    res.utils.logger.info(
                        f"Looking up the dxf piece outlines for naming for size {s} from {dxf.sizes}"
                    )
                    named_outlines = dxf.get_named_piece_image_outlines(size=s)
        except Exception as ex:
            res.utils.logger.warn(
                f"Failed to get the named pieces from the DXF - {repr(ex)}"
            )
            # raise ex
        # using the net file for getting piece names

        for name, image in enumerate_pieces(
            s3.read(self._path),
            named_piece_outlines=named_outlines,
            # confidence needs to allow for cutlines added on the IoM
            # confidence_bounds=[0, 200] ...
        ):
            # only if we have confidence should we add the name - otherwise name should be an ordinal index as before
            self._names.append(name)

            outline = get_piece_outline(image)
            outline = Polygon(outline).exterior
            # ensure that bias pieces are on the same orientation (which is: /)
            outline, image = correct_piece_orientation(outline, image, name)
            # add the hulls as the outlines for nesting
            self._piece_outlines.append(outline)

            yield image

    def load_pieces(self):
        # lazy loading - this is a bit silly for testing locally. needs refactor
        res = list(self)
        if len(self._piece_outlines) == 50:  # hack to see if this is one labels.
            self._piece_outlines = dilate_outlines_to_match(self._piece_outlines)
        return res

    @property
    def piece_outlines(self):
        if len(self._piece_outlines) == 0:
            # lazy loading - this is a bit silly for testing locally. needs refactor
            self.load_pieces()
        for ol in self._piece_outlines:
            yield ol

    @property
    def labelled_piece_images(self):
        """
        fetch the images and capture the outlines
        """
        for o in self:
            yield o

    @property
    def named_notched_piece_raw_outlines(self):
        """
        not supported in legacy because we only have what we are given in the image
        """
        return []

    @property
    def named_piece_outlines(self):
        for i, ol in enumerate(self.piece_outlines):
            yield self._names[i], ol

    @property
    def named_notched_piece_image_outlines(self):
        """
        same as outlines on the IoM but for interface completeness we add it
        """
        for i, ol in enumerate(self.piece_outlines):
            yield self._names[i], ol

    @property
    def named_labelled_piece_images(self):
        """
        fetch the images and capture the outlines
        """
        for i, o in enumerate(self.load_pieces()):
            yield self._names[i], o

    def __repr__(self):
        return f"IntelligentOneMarker: {self._path}"


def dilate_outlines_to_match(outlines, max_slop=0.05):
    # for one labels we sometimes arrive at very slightly different geometry for the pieces
    # just due to either rounding errors or some instability in the algorithm.  we can replace
    # the outlines with a hull that would contain every piece, and by doing so we get shapes which
    # are much easier to nest into a regular grid structure.
    # see also: pack_pieces._rectify_labels which can go away after this is running for a while.
    polygons = [Polygon(o) for o in outlines]
    hull = unary_union(polygons).convex_hull
    slop = max((hull - p).area / p.area for p in polygons)
    if slop <= max_slop:
        res.utils.logger.info(
            f"dilating pieces to be the same shape with slop {slop:.2f}"
        )
        return [hull.boundary] * len(outlines)
    return outlines


def correct_piece_orientation(outline, image, name):
    if not is_bias_piece(outline):
        return outline, image
    res.utils.logger.info(f"Is a bias piece: {name}")
    # note that we work with inverted piece outlines.
    # we want the un-inverted bias piece to look like / which means the inverted one should look like \
    # so the pieces we want to fix are those which when rotated 45 degrees clockwise are along the x axis.
    rotated_bounds = rotate(outline, -45).bounds
    if rotated_bounds[2] - rotated_bounds[0] < rotated_bounds[3] - rotated_bounds[1]:
        return outline, image
    res.utils.logger.info(
        f"Re-orienting piece: {name} by rotating 90 degrees clockwise"
    )
    return rotate(outline, -90), cv2.rotate(image, cv2.ROTATE_90_CLOCKWISE)


def piece_prep_request_from_print_asset(pa):
    """
    Given the print asset fetched from the schema cleaned
     airtable = res.connectors.load('airtable')
     pas = airtable.get_airtable_table_for_schema_by_name('make.print_assets')

    we will create test payloads to create prep'd pieces and update the record id on the print asset

    you can post one of these to kafka to test
    sample_pa = SELECT FROM DATA ^ e.g. dict(pas.iloc[0])
    req = piece_prep_request_from_print_asset(sample_pa)

    TOPIC = "res_meta.dxa.prep_pieces_requests"
    kafka = res.connectors.load('kafka')
    kafka[TOPIC].publish(req, use_kgateway=True)
    """

    sku = pa["sku"].split()
    bode_code = f"{sku[0][:2]}-{sku[0][2:]}"

    im_id = pa.get("intelligent_marker_file_id")

    # future we may have a uri to the marker directly
    # also think about healing use case
    marker_uri = pa.get("marker_uri")
    if im_id and not marker_uri:
        marker_uri = IntelligentOneMarker.get_marker_path(im_id)

    # this is the payload for REQUEST_TOPIC
    return {
        "id": pa["record_id"],
        "one_number": pa["order_number"],
        "body_code": bode_code,
        "material_code": pa["material_code"],
        "color_code": sku[2],
        "size_code": sku[-1],
        "piece_type": pa.get("piece_type", None),
        "uri": marker_uri,
        "metadata": {
            "intelligent_marker_id": im_id,
            "max_image_area_pixels": None,
            "print_queue_request_id": pa["record_id"],
            # needed for memory
            "file_size": None,
        },
    }


def notify_airtable(asset, fc, failure_tags=None):
    """
    We need to handle all the ways we want to update airtable
    Usually there is some queue(s) somewhere we can add a success flag or stats
    If there is a failure we should provide the reason tag
    """

    payload = {}

    try:
        airtable = res.connectors.load("airtable")
        s3 = res.connectors.load("s3")

        # apprcULXTWu33KFsh/tblwDQtDckvHKXO4w

        print_queue = airtable["apprcULXTWu33KFsh"]["tblwDQtDckvHKXO4w"]
        # for now lets use print assets as the request queue

        if asset.get("id"):
            is_meta_one = "s3://meta-one-assets-prod/styles/meta-one" in asset.get(
                "uri", ""
            )
            res.utils.logger.info(
                f"Updating airtable (print assets) with record id {asset.get('id')}"
            )
            payload = {
                "record_id": asset.get("id"),
                "Prepared Pieces Count": asset.get("piece_count"),
                # if we distinguish physical properties take them instead
                "Prepared Pieces Area": asset.get(
                    "area_pieces_yds_physical", asset.get("area_pieces_yds")
                ),
                "Prepared Pieces Nest Length": asset.get(
                    "height_nest_yds_physical", asset.get("height_nest_yds")
                ),
                "Piece Validation Flags": failure_tags,
                "Prepared Pieces Key": asset.get("piece_set_key"),
                "PPP Job Key": fc.key,
                # update has_fuse, has_stamper to alert cut in the Meta.ONE production space -> could be one or many places
                # add the factory pdf link
                # https://airtable.com/appyIrUOJf8KiXD1D/tblwIFbHo4PsZbDgz/viwH4JMhjra0nft5G?blocks=hide <- cut can be given the link to the fuse
            }

            # if is_meta_one:
            preview_paths = [p["filename"] for p in asset["make_pieces"]]

            res.utils.logger.info(
                f"Adding preview for meta one: there are {len(preview_paths)} files in the set"
            )
            purls = [s3.generate_presigned_url(f) for f in preview_paths]
            # there is also an S# file link we could use and maybe then only for meta ones
            payload["Prepared Pieces Preview"] = [{"url": purl} for purl in purls]

            ############

            # moving this here only so that we can simulate what would happen for test
            if RES_ENV != "production":
                res.utils.logger.debug(
                    f"Skipping airtable notification in env {RES_ENV}"
                )
                return

            print_queue.update_record(
                payload,
                use_kafka_to_update=False,
                RES_APP="prep_pieces",
                RES_NAMESPACE="meta",
            )

    except Exception as ex:
        # take some action
        res.utils.logger.warn(
            f"We had a problem updating airtable with payload ({payload}): {repr(ex)}"
        )


def prepare_asset_for_assembly(asset, fc=None, plan=False):
    """
    When we create pieces for printing, there are auxillary assets e.g. for cutting piece fuse or stampers etc.
    This is a good time to trigger the creation of these although we could also create a new process that does that listening to
     the response of this one

    This an isolated unit testable operation that acts on the prep print piece payload

     {
        "id": "reczQiGUrPtDB7tPT",
        "one_number": "10236934",
        "body_code": "JR-3109",
        "size_code": " 3ZZMD",
        "color_code": "SELF--",
        "material_code": "OC135",
        "uri": "s3://meta-one-assets-prod/styles/meta-one/3d/jr_3109/v5_3a9ce2c0a1/self--/3zzmd/",
        "piece_type": "self",
        "metadata": {
            "max_image_area_pixels": null,
            "intelligent_marker_id": "s3://meta-one-assets-prod/styles/meta-one/3d/jr_3109/v5_3a9ce2c0a1/self--/3zzmd/"
        }
     }
    """
    from res.flows.meta.ONE.annotation import update_assembly_asset_links
    from res.flows.meta.ONE.meta_one import MetaOne
    from res.flows.make.production_requests import get_production_record
    import traceback

    res.utils.logger.info("preparing asset for assembly")

    # if its a meta one
    try:
        asset["flow"] = "meta-one"
        piece_rank = (asset.get("metadata", {}).get("rank") or "").lower()

        if piece_rank == "healing":
            res.utils.logger.info(f"This is a healing asset - skip assembly update")
            return {}

        mone = MetaOne._try_load_from_print_payload(**asset)

        record = get_production_record(asset.get("one_number")) or {}
        assembly_request_id = record.get("record_id")
        if assembly_request_id is None:
            res.utils.logger.info(f"could not find an assembly record for this record")
            return

        res.utils.logger.info(
            f"resolved meta-one for asset with record id {assembly_request_id}"
        )

        # return update_assembly_asset_links(
        #    mone, production_record_id=assembly_request_id, plan=plan
        # )
    except Exception as ex:
        res.utils.logger.warn(
            f"Failed to prepare asset for assembly: {traceback.format_exc()}"
        )

    res.utils.logger.debug("Done updating")


def get_child_piece_info(asset, outlines_areas, marker=None, filter_key=None):
    """
    This thing can be added - the really benefit is we have s a link to the actual meta piece and an object that is a piece to make

    "type": "record",
    "name": "make_piece_assets",
    "namespace": "res_meta",
    "fields": [
        ### see fields in the avro schema
    ]
    """

    def make_key(a):
        return res.utils.res_hash(f"{a['asset_id']}{a['piece_name']}".encode())

    def _hack_outline_uri_from_file_uri(value):
        return value.replace("extract_parts", "extract_outlines").replace(
            ".png", ".feather"
        )

    def _hack_cutline_uri_from_file_uri(value):
        s = value.replace("extract_parts", "extract_cutlines").replace(
            ".png", ".feather"
        )
        return None if not res.connectors.load("s3").exists(s) else s

    res.utils.logger.debug(f"getting the piece child objects...")
    has_fusing = any(k.split("_")[0].endswith("-BF") for k in asset["pieces"].keys())
    fuser_info = get_fusing_piece_types(asset["body_code"]) if has_fusing else {}
    unmapped_fusing = []
    info = []
    for key, value in asset["pieces"].items():
        if filter_key is not None and filter_key != key:
            # skip because we are looking at a specific piece e.g. to heal filter key picks out one piece to augment here
            continue
        outline_uri = _hack_outline_uri_from_file_uri(value)
        cutline_uri = _hack_cutline_uri_from_file_uri(value)
        piece_code = "-".join(key.split("-")[3:]).split("_")[0]
        if not "NPKBYPN" in piece_code:
            piece_code = re.sub("[0-9]", "", piece_code)
        fusing_type = None
        if piece_code.endswith("-BF"):
            if piece_code not in fuser_info:
                unmapped_fusing.append(piece_code)
            else:
                fusing_type = fuser_info[piece_code]
        a = {}
        a["asset_id"] = asset.get("id")
        a["asset_key"] = asset.get("one_number")
        a["piece_name"] = key
        a["piece_id"] = make_key(a)
        a["piece_code"] = piece_code
        a["fusing_type"] = fusing_type
        a["filename"] = value
        a["outline_uri"] = outline_uri
        a["cutline_uri"] = cutline_uri
        # the unique file is fine for now but we can create something else - you want the meta-one/piece
        a["meta_piece_key"] = res.utils.res_hash(value.encode())
        a["piece_area_yds"] = outlines_areas.get(key, 0)
        a["artwork_uri"] = marker[key]["artwork_uri"]
        # for testing this going to pretend we need a block buffer but it must be knit and directional for this to be true
        a["requires_block_buffer"] = marker[key]["requires_block_buffer"] or False
        # TODO add piece type in here because we can still split e.g. block fuse out of the set
        info.append(a)
    return info


def get_fusing_piece_types(body_code):
    # lame airtable lookup for now - letting a timeout crash ppp and we retry later.
    bom_rows = Table(
        secrets_client.get_secret("AIRTABLE_API_KEY"),
        "appa7Sw0ML47cA8D1",
        "tblnnt3vhPmPuBxAF",
    ).all(
        formula=f"AND({{Body Number}}='{body_code}', {{Fusing Name}}!='', {{Generated Piece Codes}}!='')",
        fields=["Generated Piece Codes", "Fusing Name"],
    )
    pieces_fusing = {
        gpc: r["fields"]["Fusing Name"][0]
        for r in bom_rows
        for gpc in r["fields"]["Generated Piece Codes"]
    }
    res.utils.logger.info(f"Loaded fusing info from airtable: {pieces_fusing}")
    return pieces_fusing


def qualify_uri(s):
    """
    In res data platform S# uris should ALWAYS be fully qualified
    Some older code that uses boto will get the bucket and key separately which is not particularly useful
    Here depending on the env we assume the bucket that should have been there
    """
    if s[:5] != "s3://":
        if RES_ENV != "production":
            return f"s3://res-data-development/{s}"
        return f"s3://res-data-production/{s}"
    return s


def _hack_notify_brand_about_make_instance(asset, sku):
    """
    in future we need a solution to "watch" styles being made in make
    U01JDKSB196 is sirsh
    """
    try:
        rid = asset.get("id")
        slack = res.connectors.load("slack")
        # body_code = asset.get("body_code")
        # color_code = asset.get("color_code")
        # material_code = asset.get("material_code")
        # size_code = asset.get("size_code")

        # sku = f"{body_code} {material_code} {color_code} {size_code}".upper()

        owner = "Followers <@U01JDKSB196> "

        q_url = f"https://airtable.com/apprcULXTWu33KFsh/tblwDQtDckvHKXO4w/viw76IYUsx5RKjMTG/{rid}"

        s = f" {owner}  We are just printing number {asset['make_instance']} of this (special edition) style  <{q_url}|{sku}>  \n"

        p = {
            "slack_channels": ["autobots"],
            "message": s,
            "attachments": [],
        }
        slack(p)
    except Exception as ex:
        res.utils.logger.warn(f"Failing to slack - {ex}")


def try_get_experimental_marker(asset, is_3d=True, is_healing=False, is_extra=False):
    def try_slack_event(asset):
        try:
            rid = asset.get("id")
            slack = res.connectors.load("slack")
            body_code = asset.get("body_code")
            color_code = asset.get("color_code")
            material_code = asset.get("material_code")
            size_code = asset.get("size_code")
            one_number = asset.get("one_number")

            sku = f"{body_code} {material_code} {color_code} {size_code}".upper()

            owner = "Followers <@U01JDKSB196> "

            q_url = f"https://airtable.com/apprcULXTWu33KFsh/tblwDQtDckvHKXO4w/viw76IYUsx5RKjMTG/{rid}"

            s = f"<{q_url}|{sku}>  is passing through PPP - {one_number}\n"

            p = {
                "slack_channels": ["autobots"],
                "message": s,
                "attachments": [],
            }
            slack(p)
        except Exception as ex:
            res.utils.logger.warn(f"Failing to slack - {ex}")

    res.utils.logger.info(f"Looking for an experimental marker")

    def extract_sku_from_uri(sinput):
        """
        uri = 's3://meta-one-assets-prod/styles/meta-one/JR-3110 LTCSL SELF-- 3ZZMD/9'
        uri = 's3://meta-one-assets-prod/styles/meta-one/JR-3110-LTCSL-BLACK1-3ZZMD/9'
        uri = 's3://res-data-production/flows/v1/dxa-prep_ordered_pieces/primary/extract_parts/CC-3054-ECVIC-MAGEHQ-5ZZXL-V10-10325873-EC238C2000/CC-3054-V10-SHTNKCLSUN-BF.png'

        """
        sinput = sinput.split("/")[-2]
        if "SELF--" in sinput:
            sinput = sinput.replace("SELF--", "<<COLOR_TOKEN>>")

        normed = sinput if sinput[2] != "-" else sinput[:2] + sinput[3:]
        sku = " ".join(normed.replace("-", " ").split(" ")[:4])
        if "<<COLOR_TOKEN>>" in sku:
            sku = sku.replace("<<COLOR_TOKEN>>", "SELF--")
        if sku[2] != "-":
            sku = f"{sku[:2]}-{sku[2:]}"
        return sku.replace("  ", " ").strip()

    order_email = asset.get("metadata", {}).get("order_item_order_email")

    try:
        # from res.flows.meta.ONE.meta_one_2d import MetaOne2d
        from res.flows.meta.ONE.meta_one import MetaOne

        if is_3d:
            body_code = asset.get("body_code", "").replace("-", "")

            if (
                order_email == "sirsh@resonance.nyc"
            ):  # or maybe some other sort of order type we capture
                res.utils.logger.info("in hack mode")
                # asset["fully_annotate"] = True
                asset["healz_mode"] = True
            # meta_data.order_item_order_email -> if sew-dev going to do a full annotation

            if not asset.get("sku"):
                if body_code in asset.get("uri").replace("-", ""):
                    uri = asset.get("uri")
                    sku = extract_sku_from_uri(uri)
                    res.utils.logger.info(
                        f"From uri {asset.get('uri')} resolved sku {sku}"
                    )
                    asset["sku"] = sku
                else:
                    # this is unfortunate because we should pass it in the payload instead - note the old meta one uses the three part sku
                    sku = (
                        MetaMarker(asset.get("uri")).sku
                        + " "
                        + asset["size_code"].strip()
                    )
                    asset["sku"] = sku
        try:
            # make instance is the ranked one number and sku combination - how many times did we make this sku
            asset["make_instance"] = get_one_number_style_sku_order_rank(
                one_number=asset["one_number"], style_sku=asset["sku"]
            ).iloc[0]["sku_order_rank"]
            res.utils.logger.info(
                f"PPP assigned make instance {asset['make_instance']}"
            )

            # hack special
            if (
                asset.get("body_code") in ["JR-3118", "JR-3117"]
                and asset["make_instance"] % 25 == 0
            ):
                res.utils.logger.info(f"Special edition notification")
                _hack_notify_brand_about_make_instance(asset, sku)

        except Exception as ex:
            res.utils.logger.warn(
                f"Did not get the asset sku rank for this one number for asset {asset} - {ex}"
            )

        mone_selector = dict(asset)
        if is_healing and mone_selector.get("piece_type"):
            # we remove this so that we select all pieces to find the healing piece
            mone_selector.pop("piece_type")
        if is_healing and mone_selector.get("material_code"):
            # we remove this so that we select all pieces to find the healing piece
            mone_selector["material_code"] = None

        mone = MetaOne._try_load_from_print_payload(
            **mone_selector, is_healing=is_healing, is_extra=is_extra
        )

        # if the meta one is a single label panel - reload the meta one and tell it to multiply by 50
        if mone.is_label() and not is_healing:
            mone_selector["multiplier"] = 50
            mone = MetaOne._try_load_from_print_payload(
                **mone_selector, is_healing=is_healing, is_extra=is_extra
            )

        res.utils.logger.info(f"<<<<< We loaded a meta one for this request >>>>>")

        # if order_email != "sirsh@resonance.nyc":
        try_slack_event(asset)
        # else:
        #    res.utils.logger.info("Sirsh im skipping the slack relay")

        return mone
    except Exception as ex:
        res.utils.logger.warn(
            f"Error ocurred in loading marker {traceback.format_exc()}"
        )
        raise ex


def flag_print_asset(asset_record_id, flag_reason, tag):
    BASE_RES_MAGIC_PRINT = "apprcULXTWu33KFsh"
    TABLE_PRINT_ASSETS_FLOW = "tblwDQtDckvHKXO4w"

    airtable = res.connectors.load("airtable")
    airtable[BASE_RES_MAGIC_PRINT][TABLE_PRINT_ASSETS_FLOW].update_record(
        {
            "record_id": asset_record_id,
            "Flag For Review": "true",
            "Flag For Review Reason": flag_reason,
            "Print Flag for Review: Tag": tag,
        },
        use_kafka_to_update=False,
    )


def _handle_print_pieces(
    asset, fc, should_resolve_material_props=False, mone=None, skip_relays=False
):
    """
    filter the meta one by
    - piece_types
    - piece_codes
    - material_code(s)

    Then generate all the digital assets that we need for those pieces and return stats

    should_resolve_material_props is added for testing convenience - normally these are resolved in batch before this method is called
    ...
    """

    images.text.ensure_s3_fonts()
    images.icons.extract_icons()
    from warnings import filterwarnings

    filterwarnings("ignore")  # shapely warnings are annoying

    # default here - we are adding this and lots of stats
    asset["pieces"] = {}

    s3 = res.connectors.load("s3")

    piece_rank = (asset.get("metadata", {}).get("rank") or "").lower()
    order_type = (asset.get("metadata", {}).get("order_type") or "").lower()

    # temp allow sku in there too.
    is_meta_one = "s3://meta-one-assets-prod/styles/meta-one" in asset.get(
        "uri", ""
    ) or asset.get("body_code", "").replace("-", "") in asset.get("uri")

    res.utils.logger.info(f"IS META ONE: {is_meta_one}")

    mone = mone or get_make_one_request(asset["one_number"])

    # we only swap soft swap if the asset is not requested in this material
    # TODO remove hack
    if asset["material_code"] in ["CTW70"]:
        res.utils.logger.info(f"<<<<<<<< DISABLE SOFT SWAP FOR THIS MATERIAL >>>>>>>>>")
        os.environ["DISABLE_SOFT_SWAPS"] = "True"
    else:
        res.utils.logger.info(
            f"<<<<<<<< CHECKED AND WILL NOT DISABLE SWAP FOR THESE MATERIALS IF ACTIVE >>>>>>>>>"
        )

    if piece_rank == "healing":
        try:
            # res.utils.logger.warn(
            #     f"Because we are HEALING we do not generate new assets"
            # )
            if mone["is_extra"]:  # mone["is_photo"] or
                message = "This piece is being held until specifically requested."
                if mone["is_photo"]:
                    message += " This piece is from a PHOTO sales channel order."
                if mone["is_extra"]:
                    message += " This piece was ordered as an extra."

                res.utils.logger.info(
                    f"This piece should be held and not healed straight away"
                )

                flag_print_asset(asset["id"], message, "Hold for demand")
        except:
            pass

        asset["piece_count"] = 1

        # kludge.
        if asset["uri"].endswith("-BF.png") and asset["piece_type"] != "block_fuse":
            res.utils.logger.info(
                f"Kludging piece type to block_fuse in light of uri {asset['uri']}"
            )
            asset["piece_type"] = "block_fuse"
        try:
            # this is some kind of legacy thing that might not matter but I dont want to find out the hard way.
            file = qualify_uri(str(asset["uri"]))
            k = file.split("/")[-1].split(".")[0]

            # pull the sku from make one production request
            asset["sku"] = mone["sku"]

            res.utils.logger.info(
                f"Re saving healing file from marker (not the outline): {k}:{file} for sku {asset.get('sku')}"
            )

            one_marker = try_get_experimental_marker(
                asset, is_3d=is_meta_one, is_healing=True
            )
            ###### PASS the filter key

            s3.write(file, one_marker[k].labelled_piece_image)
            res.utils.logger.info(f"Saved to {file}")

            res.utils.logger.info(f"Using healing piece uri for stats: {k}:{file}")

            asset["pieces"] = {k: file}

            #  get healed piece statistics - we can use the intelligent one marker class which just treats any image like a marker
            marker = IntelligentOneMarker(file)
            asset.update(marker.get_nesting_statistics())

            if not marker:
                marker_path = asset["metadata"]["marker_file_path"]
                if not marker_path:
                    file_id = asset["metadata"]["intelligent_marker_id"]
                    if file_id:
                        marker_path = IntelligentOneMarker.get_marker_path(file_id)
                if marker_path:
                    factory = (
                        MetaMarker
                        if "s3://meta-one-assets-prod/styles/meta-one" in marker_path
                        else IntelligentOneMarker
                    )
                    marker = factory(path=marker_path, order_type=order_type, **asset)
            piece_areas = {
                str(name): pixels_to_yards(Polygon(ol).area, 2)
                for name, ol in marker.named_notched_piece_image_outlines
            }
            asset["make_pieces"] = get_child_piece_info(
                asset, piece_areas, marker=one_marker or marker, filter_key=k
            )

            asset["piece_set_key"] = marker.key
            # use the one marker for extended piece info if we have it
            # try to add in make-pieces about the asset - and also the assets key...

            res.utils.logger.info("Processed healing asset")
        except Exception as ex:
            res.utils.logger.warn(
                f"Failed to add healing piece statistics {res.utils.ex_repr(ex)}"
            )

        return asset

    if should_resolve_material_props:
        asset = _resolve_material_properties(asset)

    # we are going to default to the m1 if we have it in 3d but we will monitor
    marker = try_get_experimental_marker(
        asset, is_3d=is_meta_one, is_extra=mone.get("is_extra")
    )
    is_experimental = False
    if not marker:
        factory = MetaMarker if is_meta_one else IntelligentOneMarker
        marker = factory(path=asset["uri"], order_type=order_type, **asset)
    else:
        res.utils.logger.info(f"Found an experimental marker {marker}")
        asset["metadata"]["using_db_meta_one"] = True
        is_experimental = True
    # test functions only return the filtered pieces or compute statistics for them

    # Contract/flow pattern -> the asset can be invalidated and we raise an error that triggers asset review
    if not marker.is_valid:
        fc.metric_incr(
            verb="EXITED",
            group=getattr(marker, "style_sku", asset.get("body_code")),
            status="INVALIDATED_ASSET",
        )
        notify_airtable(asset, fc=fc, failure_tags=["INVALIDATED_META_ONE"])
        raise AssetFlaggedFlowException(
            f"The marker {marker.key} was invalidated ",
            flow_context=fc,
        )

    marker_key = marker.key

    # record for searchability
    asset["piece_set_key"] = marker_key

    def validate_piece(o):
        material_props = asset.get("metadata", {})
        cuttable_width_px = material_props.get("output_bounds_width")
        width = o.bounds[2] - o.bounds[0]

        if cuttable_width_px and width >= cuttable_width_px:
            notify_airtable(asset, fc=fc, failure_tags=["PIECE_TOO_WIDE"])
            raise FlowException(
                "TODO add stronger type to this contract violation", flow_context=fc
            )

        return True

    key_from = lambda x: str(x).split("/")[-1].split(".")[0]

    outlines_areas = {}
    image_bounds = {}

    def get_cutlines(marker_ref, **kwargs):
        for name, ol in marker_ref.named_notched_piece_raw_outlines:
            yield NamedFlowAsset(name, np.array(swap_points(ol)))

    def get_outlines(marker_ref, **kwargs):
        # TODO: if we passed in the shape outlines we can make sure there were no symmetry or other mismatch bugs
        # for name, ol in marker_ref.named_piece_outlines:
        # this is slower but we have to take of the buffered image and any image with physical notches
        # to nest properly in general
        for name, ol in marker_ref.named_notched_piece_image_outlines:
            # capture outlines areas in square yards
            outline_poly = Polygon(ol)
            outlines_areas[key_from(name)] = pixels_to_yards(outline_poly.area, 2)
            # sanity check that the polygon stands a chance of containing the image.
            x0, y0, x1, y1 = outline_poly.bounds
            h, w = image_bounds[key_from(name)]
            if h > (y1 - y0) + PIECE_IMAGE_SLOP or w > (x1 - x0) + PIECE_IMAGE_SLOP:
                raise ValueError(
                    f"Piece {name} with bounds {x0, y0, x1, y1} cannot contain image with dimensions {w, h}"
                )

            if validate_piece(ol):
                res.utils.logger.debug(f"Loaded outline: {name}")
                # make res nest compatible and return
                yield NamedFlowAsset(name, np.array(swap_points(ol)))

    def get_labelled_parts(marker_ref, **kwargs):
        for name, image in marker_ref.named_labelled_piece_images:
            res.utils.logger.debug(f"Loaded labelled piece image {name}")
            # the image can be a np array or a PIL.Image evidently.
            image_bounds[key_from(name)] = (
                image.shape[0:2]
                if isinstance(image, np.ndarray)
                else (image.height, image.width)
            )
            yield NamedFlowAsset(name, image)

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

    # TODO: we can probe the number of pieces we already have and skip if the cache is valid
    # if we want to do outlines first we should refactor IoM to fetch both at same time OR validate internally to avoid images in memory

    def delete_max_files(files, maximum=200):
        """
        a bit of paranoid behaviour - ill purge but only if there are fewer pieces than in a big marker
        """
        l = list(files)
        if len(l) < maximum:
            res.utils.logger.info(f"purging {l} items from list at  ")
            s3.delete_files(l)

    if not is_experimental:
        delete_max_files(
            fc.get_node_file_names(node="extract_parts", keys=[marker_key])
        )
        delete_max_files(
            fc.get_node_file_names(node="extract_outlines", keys=[marker_key])
        )

    fc.apply(
        node="extract_parts",
        func=get_labelled_parts,
        data=marker,
        key=marker_key,
    )
    fc.apply(
        node="extract_outlines",
        func=get_outlines,
        data=marker,
        key=marker_key,
    )
    fc.apply(
        node="extract_cutlines",
        func=get_cutlines,
        data=marker,
        key=marker_key,
    )

    names = fc.get_node_file_names(node="extract_parts", keys=[marker_key])

    res.utils.logger.debug("Adding pieces map...")
    # we  add the named pieces to the response and also where the piece images are
    asset["pieces"] = {key_from(n) or "no_file_name": n for n in names}
    if hasattr(marker, "piece_keys"):
        # filter - safety - e.g. some case of combo pieces on the path but not in the marker
        res.utils.logger.info("filtering pieces for marker....")
        asset["pieces"] = {
            k: v for k, v in asset["pieces"].items() if k in marker.piece_keys
        }

    # we can add make pieces here which are super interesting things for tracking purposes
    asset["make_pieces"] = get_child_piece_info(asset, outlines_areas, marker=marker)

    # these are prepared for cut - i think this is redundant and has not been removed
    # try:
    #     if is_meta_one:
    #         res.utils.logger.info(f"notched outlines generation....")
    #         fc.apply(
    #             node="notched_outlines",
    #             func=get_filtered_notched_outlines,
    #             data=marker,
    #             key=marker_key,
    #         )
    # except Exception as ex:
    #     res.utils.logger.warn(
    #         f"Failed an experimental step of finding notched outlines: {repr(ex)}"
    #     )

    s = marker.get_nesting_statistics()

    if not isinstance(s, dict):
        if isinstance(s, pd.DataFrame):
            # TODO: actually we need to aggregate this
            s = s.to_dict("records")[0]

    # there is a back compat thing we do do add physical as the default no-suffix value

    for field in [
        "height_nest_yds",
        "width_nest_yds",
        "area_nest_yds",
        "area_nest_utilized_pct",
        "area_pieces_yds",
    ]:
        # the "default" is the physical value
        s[field] = s.get(f"{field}_physical")

    # hack to mark labels as block fusing.
    if len(names) == 50 and s["area_pieces_yds"] < 1:
        res.utils.logger.info("Marking pieces for the block fuser.")
        s["piece_type"] = "block_fuse"

    asset.update(s)

    return asset


def _find_in_assembly(assets):
    # logic placeholder if we need to batch resolve production keys

    return assets


def _resolve_material_properties(assets):
    """
    get the material properties for the material code
    these are used for nesting checks e.g. physical material props
    there is a dep on something we have not finalized which is the schema system now using s3

    NOTE THAT The attributes added here as applied in the request are merged into the response - we do not re-resolve these attributes in the response
    """
    from res.connectors.airtable import AirtableConnector

    def fix_type(t):
        if isinstance(t, list):
            return t
        if pd.isnull(t):
            return None
        return t

    renormed = False
    if not isinstance(assets, list):
        assets = [assets]
        renormed = True

    assets = pd.DataFrame(assets)
    airtable = res.connectors.load("airtable")
    filters = AirtableConnector.make_key_lookup_predicate(
        list(assets["material_code"]), "Material Code"
    )
    mat_props = airtable.get_airtable_table_for_schema_by_name(
        "make.material_properties", filters
    )

    mat_props = mat_props.to_dict("records")
    mat_props = {d["key"]: d for d in mat_props}

    assets = assets.to_dict("records")
    for a in assets:
        props = mat_props[a["material_code"]]
        metadata = a.get("metadata", {})
        metadata["cuttable_width"] = props["cuttable_width"]
        metadata["output_bounds_width"] = DPI * props["cuttable_width"]
        metadata["stretch_x"] = props["compensation_width"]
        metadata["stretch_y"] = props["compensation_length"]
        metadata["paper_marker_stretch_x"] = props["paper_marker_compensation_width"]
        metadata["paper_marker_stretch_y"] = props["paper_marker_compensation_length"]
        metadata["offset_size"] = props["offset_size"]
        metadata["material_stability"] = props["material_stability"]
        metadata["buffer"] = DEFAULT_NEST_BUFFER
        metadata["fabric_type"] = props["fabric_type"]

        metadata = {k: fix_type(v) for k, v in metadata.items()}

        # just in case it was empty
        a["metadata"] = metadata

    return assets if not renormed else assets[0]


def handle_payload(asset, skip_relays=True, prep_assembly_asset=False, **kwargs):
    """
    pass in prepare_asset_for_assembly=False to not attempt posting the pdf etc.
    handle payloads is a local dev tool so we skip relaying to kafka and airtable by defailt
    """
    e = res.flows.FlowEventProcessor().make_sample_flow_payload_for_function(
        "dxa.prep_ordered_pieces"
    )
    e["assets"] = [asset]
    return handler(
        e, prep_assembly_asset=prep_assembly_asset, skip_relays=skip_relays, **kwargs
    )


def remove_ppp_failed_m1_contract_if_exists(mone):
    try:
        from res.flows.meta.ONE.contract.ContractVariables import ContractVariables

        df = ContractVariables.load(base_id="appH5S4hIuz99tAjm")
        crid = dict(df[df["variable_name"] == "Prep pieces preview Failed"].iloc[0])[
            "record_id"
        ]
        existing = mone.get("contractVariableId") or []
        if crid in existing:
            existing = [e for e in existing if e != crid]
            res.connectors.load("airtable")["appH5S4hIuz99tAjm"][
                "tblptyuWfGEUJWwKk"
            ].update_record({"record_id": mone["id"], "Contract Variables": existing})
            res.utils.logger.info(
                f"Removed the contract variable for PPP failure from make one production contract vars"
            )
    except Exception as ex:
        res.utils.logger.warn(f"failed to update the mone contract variables {ex}")
        pass


def add_ppp_failed_m1_contract_if_not_exists(mone):
    try:
        from res.flows.meta.ONE.contract.ContractVariables import ContractVariables

        df = ContractVariables.load(base_id="appH5S4hIuz99tAjm")
        crid = dict(df[df["variable_name"] == "Prep pieces preview Failed"].iloc[0])[
            "record_id"
        ]
        existing = mone.get("contractVariableId") or []

        if crid not in existing:
            existing = [crid] + existing
            res.connectors.load("airtable")["appH5S4hIuz99tAjm"][
                "tblptyuWfGEUJWwKk"
            ].update_record({"record_id": mone["id"], "Contract Variables": existing})
            res.utils.logger.info(
                f"Added the contract variable for PPP failure to make one production contract vars"
            )
    except Exception as ex:
        res.utils.logger.warn(f"failed to update the mone contract variables {ex}")
        pass


# for testing set a sizable bit of memory but we can set this on demand in the metadata per asset...
@flow_node_attributes(memory="50Gi", allow24xlg=True, disk="5G")
def handler(event, context={}, prep_assembly_asset=True, skip_relays=False):
    # TODO read config e.g. how to handle things per env like should we send fatal errors to pager duty

    with FlowContext(event, context) as fc:
        kafka = fc.connectors["kafka"]

        for asset in fc.assets:
            try:
                if asset.get("flow") == "cache_only":
                    """
                    use this pre state for testing/pre caching the format we need
                    but do not do any of the relays to kafka or airtable - no side effects except the cache
                    """
                    e = cache_handler(asset)
                    continue

                if asset.get("flow") == "v2":
                    """
                    the new process for the flow will do everything here
                    """
                    e = make_handler(asset, ppp_key=fc.key)
                    """
                    we may not do everything above but contractually we need to 
                    - update airtable assetes (some of this can move upstream)
                    - update airtable contract failures - refactor and move
                    - any nesting related mods we do need to move
                    - we should get the response contract e and post to ppp responses
                    - we should handle the piece tracking inline and think about all that piece instance stuff
                    """
                    continue

                mone = get_make_one_request(asset["one_number"])

                asset = _handle_print_pieces(
                    asset, fc, mone=mone, skip_relays=skip_relays
                )

                # also prepare queues for cut
                # PREPARE FOR ASSEMBLY
                if prep_assembly_asset:
                    prepare_asset_for_assembly(asset)
                # generate piece statistic record e.g. theoretical numbers links e.g the nesting of this piece set
                # publish status somewhere
                # unflag the asset if necessary.
                unflag_asset(
                    asset["id"],
                    flag_reason_predicate=lambda r: "Prep pieces failed" in r,
                )

                remove_ppp_failed_m1_contract_if_exists(mone)

            except Exception as ex:
                error_message = f"Prep pieces failed for asset {asset.get('id')}/{asset.get('unit_key')}. Trace: {traceback.format_exc()}"
                res.utils.logger.warn(error_message)

                ping_slack(
                    f"[PPP] <@U0361U4B84X> Failed for asset {asset['one_number']} ({fc.key}): ```{traceback.format_exc()}```",
                    "autobots",
                )

                fc.log_asset_exception(asset, ex, datagroup="body_code")
                try:
                    add_ppp_failed_m1_contract_if_not_exists(mone)

                    flag_asset(
                        asset["one_number"],
                        asset["id"],
                        asset["uri"],
                        fc._task_key,
                        f"{error_message} (argo logs at {fc.log_path})",
                    )
                    # see: https://airtable.com/apprcULXTWu33KFsh/tbldADFytseIivUcO/viwahE5ntI1TsLeZ6/recAkJ90AQiWOz6DG?blocks=hide
                    add_contract_variables_to_assets(
                        [asset["id"]], ["recAkJ90AQiWOz6DG"]
                    )

                except:
                    # because we are notifying airtable and anything can happen - i want to continue
                    pass
                continue

            if not skip_relays:
                try:
                    asset["created_at"] = res.utils.dates.utc_now_iso_string()
                    asset["metadata"]["logs"] = fc.log_path
                    kafka[RESPONSE_TOPIC].publish(asset, use_kgateway=True, coerce=True)
                except Exception as ex:
                    res.utils.logger.warn(
                        f"FAILED TO UPDATE kafka sink {res.utils.ex_repr(ex)}"
                    )
                # simulate what we would do in the new system to register pieces at the roll packing stage
                _ppp_move_pieces_to_roll_packing_enter(asset)

                notify_airtable(asset, fc=fc)

    return {}


def has_large_swatch(sku):
    """
    For sizes likes 0Y015, 0Y014 ... we need lots of memory
    """
    g = [f"0Y{str(i).zfill(3)}" for i in range(6, 16)]
    size_code = sku.split(" ")[-1].strip()
    return size_code in g


def generator(event, context={}):
    with FlowContext(event, context) as fc:
        if len(fc.assets):
            res.utils.logger.info(
                f"Assets supplied by payload - using instead of kafka"
            )
            assets = fc.assets_dataframe
        elif fc.args.get("ppp_from_pa", False):
            res.utils.logger.info("Retrying PPP for missing assets")
            assets = pd.DataFrame(ppp_from_pa())[0:20]
        else:
            res.utils.logger.info(f"Consuming from topic {REQUEST_TOPIC}")
            kafka = fc.connectors["kafka"]
            assets = kafka[REQUEST_TOPIC].consume()

        """
        asset specific memory provider passed into the asset payload creator below
        """
        metadata_provider = (
            lambda asset: {
                "memory": "50G",
                "asset_key": asset.get("sku"),
            }
            if not has_large_swatch(asset.get("sku"))
            else {
                "memory": "250G",
                "asset_key": asset.get("sku"),
            }
        )

        if len(assets):
            res.utils.logger.info(f"Adding the sku ranks")
            # *** where we get the sku mapping for all the ones in the set ***
            style_codes = assets["sku"].map(make_style_code).unique()
            one_numbers = assets["one_number"].unique()
            mapping = get_style_code_ranks_for_style_codes(
                list(style_codes), one_number_hint=one_numbers
            )

            assets["style_make_sequence_number"] = (
                assets["one_number"].map(int).map(lambda x: mapping.get(x))
            )
            # ***                                                           ****

            assets = assets.to_dict("records")

        if len(assets) > 0:
            assets = _resolve_material_properties(assets)
            assets = _find_in_assembly(assets)
            assets = fc.asset_list_to_payload(
                assets, metadata_provider=metadata_provider
            )
        # resolve any airtable record ids to update queues here and add to metadata of the asset

        # do some cleaning / may implement write&restore from s3 for large work
        # add metadata.cluster.memory
        # use metadata.file_size to determine memory required

        res.utils.logger.info(f"returning the work from the generator - {assets} ")
        return assets.to_dict("records") if isinstance(assets, pd.DataFrame) else assets


def test_update_assembly(event, context={}):
    from res.flows.meta.marker import MetaMarker

    mmm = MetaMarker(
        "s3://meta-one-assets-prod/styles/meta-one/3d/kt_2020/v5_ff019ae209/bonelg/ptz04/",
        one_number="10240551",
    )

    mmm.update_assembly_asset_links("rec0l7j0xsBJvtYMQ", plan=True)


def reducer(event, context={}):
    return {}


def _payload_template(version="resmagic.io/v1", **kwargs):
    from res.flows import payload_template

    p = payload_template("dxa.prep_ordered_pieces", version, **kwargs)
    assets = kwargs.get("assets")
    if not isinstance(assets, list):
        assets = [assets]
    p["assets"] = assets

    return p


"""
dev helper
"""


# default to the meta-one flow especially when retrying ppp - since this may cover up some problems in v2 for now.
def ppp_from_pa(event={}, context={}, data=None, record_ids=[], ppp_flow="v2"):
    """
    By default do not pass in anything and used a hard code view that should show things missing original ppp requests in print assets table
    any predicates can be used once we map to our schema using the airtable connector

    This resolves a few things from the related one production request but essentially maps the print asset back into a payload

    you can use these to resbumit on prod e.g.

    assets = ppp_from_pa()
    os.environ['KAFKA_KGATEWAY_URL'] = 'https://data.resmagic.io/kgateway/submitevent'
    for asset in assets:
        kafka['res_meta.dxa.prep_pieces_requests'].publish(asset, use_kgateway=True)
        #pass

    """
    from res.connectors.airtable import AirtableConnector
    import json

    def _from_data(pa, mo):
        def body_code_from_sku(x):
            b = x.split(" ")[0].strip()
            if b[2] != "-":
                b = f"{b[:2]}-{b[2:]}"
            return b

        def sku_part(x, i):
            return x.split(" ")[i].strip()

        seq_numbers = (
            pa.get("ppp_spec_json")
            if not pd.isnull(pa.get("ppp_spec_json"))
            and not pd.isna(pa.get("ppp_spec_json"))
            else []
        )

        if isinstance(seq_numbers, str):
            seq_numbers = json.loads(seq_numbers)

        if isinstance(seq_numbers, dict):
            seq_numbers = [seq_numbers]

        # zzzzzzzzzzzzzzz
        sku_rank = pa.get("sku_rank")
        if isinstance(sku_rank, list):
            sku_rank = sku_rank[0]
        if sku_rank is None or pd.isna(sku_rank):
            sku_rank = -1

        d = {
            "id": pa["record_id"],
            "one_number": pa["order_number"],
            "body_code": body_code_from_sku(pa["sku"]),
            "body_version": str(mo.get("body_version")),
            "sales_channel": mo.get("sales_channel"),
            "upstream_request_id": mo.get("record_id"),
            "size_code": sku_part(pa["sku"], 3),
            "color_code": sku_part(pa["sku"], 2),
            "material_code": pa["material_code"],
            "sku": pa["sku"],
            "piece_type": pa.get("pieces_type") or "self",
            "piece_sequence_numbers": seq_numbers,
            "sku_make_sequence_number": int(sku_rank),
            "metadata": {
                "rank": pa["rank"],
                "intelligent_marker_id": pa["s3_file_uri"],
                "order_item_id": mo.get("order_line_item_id"),
                #       "order_item_order_email": "struko@me.com",
                #       "order_item_createdAt": "2023-01-07T16:42:58.000Z",
                # "order_item_channelOrderLineItemId": "11576964972627",
                "order_item_order_number": mo["order_number"].split("-")[-1]
                if "order_number" in mo
                else None,
            },
        }

        if ppp_flow is not None:
            d["flow"] = ppp_flow

        return d

    data = (
        data
        if data is not None
        else AirtableConnector.get_airtable_table_for_schema_by_name(
            "make.print_assets",
            filters=AirtableConnector.make_key_lookup_predicate(
                record_ids, "_record_id"
            ),
            remmove_pseudo_lists=False,
        )
        if len(record_ids) > 0
        else AirtableConnector.get_airtable_table_for_schema_by_name(
            "make.print_assets", view="viwTeypjOFv8ehY1I"
        )
    )
    mos = AirtableConnector.get_airtable_table_for_schema_by_name(
        "make.production_requests",
        filters=AirtableConnector.make_key_lookup_predicate(
            list(data["order_number"]), "Order Number v3"
        ),
    )
    mos = {record["key"]: record for record in mos.to_dict("records")}

    payloads = []
    for pa in data.to_dict("records"):
        mo = mos.get(pa["order_number"], {})
        payload = _from_data(pa, mo)
        payloads.append(payload)
    return payloads


def get_make_one_request(rid="10314879"):
    QUERY_MAKE_ONE_REQUEST = """
    query getMakeOneRequest($one_number:String!){
        makeOneProductionRequest(orderNumber:$one_number){
            id
            name
            sku
            type
            bodyVersion
            requestName
            originalOrderPlacedAt
            orderNumber
            countReproduce
            reproducedOnesIds
            salesChannel
            belongsToOrder
            flagForReviewTags
            contractVariableId
            sewContractVariableId
        }
    }
    """
    gql = res.connectors.load("graphql")
    make_request_response = (
        gql.query(QUERY_MAKE_ONE_REQUEST, {"one_number": rid})["data"][
            "makeOneProductionRequest"
        ]
        or {}
    )

    make_request_response["is_extra"] = "Extra Prod Request" in str(
        make_request_response.get("flagForReviewTags", [])
    )
    make_request_response["is_photo"] = (
        "photo" in make_request_response.get("salesChannel", "").lower()
    )
    if "optimus" in make_request_response.get("salesChannel", "").lower():
        make_request_response["is_extra"] = True
    return make_request_response


def safe_post_prep_print_pieces_for_request(
    request_id, murl=None, marker_id=None, plan=False, env="production", tries=5
):
    """

    request_id:
      Add a print asset request id from https://airtable.com/apprcULXTWu33KFsh/tblwDQtDckvHKXO4w/viwQM8wZ2prepWMkV?blocks=hide

    env:
    we can post to dev or prod envs to test creating print prep piece requests

    these will be backup on a 5 minute interval for prep-print-pieces

    import res


    safe_post_prep_print_pieces_for_request('recUcTavFrvWeimX0',
                                        's3://resmagic/uploads/5c9a526c-69de-40cc-b7ec-a80e1797a2c0.png',
                                        marker_id='62759bd53c68ad000953ac5d',
                                        env='development',
                                        plan=False)

    """
    import requests
    import time
    from .graph_queries import GET_PRINT_ASSET_REQUEST
    from res.connectors.airtable import AirtableConnector

    marker_id = marker_id or murl

    q = res.connectors.load("graphql")
    d = q.query_with_kwargs(GET_PRINT_ASSET_REQUEST, id=request_id)
    req = d["data"]["printAssetRequest"]

    airtable = res.connectors.load("airtable")
    tab = airtable["apprcULXTWu33KFsh"]["tblwDQtDckvHKXO4w"]

    # lookup the request print asset that has been generated and get its
    try:
        res.utils.logger.info(f"Requesting the print asset {request_id}")
        prec = dict(
            tab.to_dataframe(
                filters=AirtableConnector.make_key_lookup_predicate(
                    request_id, "_record_id"
                )
            ).iloc[0]
        )

        murl = murl or prec.get("S3 File URI")
        marker_id = marker_id or prec.get("Intelligent ONE Marker File ID")
    except Exception as ex:
        res.utils.logger.warn(
            f"Failed to resolve the print asset IoM detail: {repr(ex)}"
        )

    try:
        style_code = req["makeOneProductionRequest"]["style"]["code"]
        body = style_code.split(" ")[0].rstrip().lstrip()
        color = style_code.split(" ")[2].rstrip().lstrip()

        piece_type = None

        assets = {
            "id": req["id"],
            "one_number": req["makeOneProductionRequest"]["orderNumber"],
            "body_code": body,
            "material_code": req["material"]["code"],
            "color_code": color,
            "size_code": req["makeOneProductionRequest"]["size"]["code"],
            "piece_type": None,
            "uri": murl,
            "metadata": {
                "intelligent_marker_id": marker_id,
                "max_image_area_pixels": None,
                "rank": req["marker"]["rank"],  # take rank from 2d marker
            },
        }
        if plan:
            return assets

        domain = "data" if env == "production" else "datadev"
        url_scheme = "https" if env == "development" else "http"

        url = f"{url_scheme}://{domain}.resmagic.io/kgateway/submitevent"
        topic = "res_meta.dxa.prep_pieces_requests"
        data = {
            "version": "1.0.0",
            "process_name": "marker_fuction",
            "topic": topic,
            "data": assets,
        }

        res.utils.logger.debug(f"posting to url {url} (retries= {tries}): {data}")
        response = None

        for i in range(tries):
            try:
                response = requests.post(url, json=data)
                break
            except:
                time.sleep(1)
                print("retry", i + 1)

        if not response or response.status_code != 200:
            print(
                "FAILED TO POST KAFKA:", response.text if response else "CANNOT CONNECT"
            )
            return assets

        res.utils.logger.debug("POSTED OK")
        return assets

    except Exception as ex:
        print("failed but can we check", ex)
        raise ex


def query_queues(one_numbers=None, skus=None, env="IAMCURIOUS_PRODUCTION"):
    Q = f"""
        SELECT a.*, b."piece_count" as "response_piece_count", b."created_at" as "response_created_at" FROM {env}.PREP_PIECES_REQUESTS_QUEUE a
          LEFT JOIN {env}.PREP_PIECES_RESPONSES_QUEUE b 
            on a."id" = b."id"
        """
    # TODO if one numbers,skus etc we can filter

    snowflake = res.connectors.load("snowflake")

    return snowflake.execute(Q)


def get_recent_production_errors(
    since_days_ago=3,
    path="s3://res-data-production/flows/v1/dxa-prep_ordered_pieces/primary/errors/",
):
    """
    fetch errors that are saved in s3 when fatal and uncaught

    check by count
        df.groupby('exception').count()

    reproduce on dev

        some_asset_from_df = df.iloc[0]['asset']
        TOPIC = "res_meta.dxa.prep_pieces_requests"
        kafka[TOPIC].publish(some_asset_from_df, use_kgateway=True)

    """
    from res.utils.dataframes import expand_column

    s3 = res.connectors.load("s3")

    def read_with_date(info):
        d = s3.read(info["path"])
        d["last_modified"] = info["last_modified"]
        return d

    df = pd.DataFrame(
        [
            read_with_date(f)
            for f in s3.ls_info(
                path,
                modified_after=res.utils.dates.relative_to_now(since_days_ago),
            )
        ]
    )
    # df.style.set_properties(subset=['exception'], **{'width-min': '500px'})

    df = expand_column(df, "asset")

    return df


def download_for_cluster_key(key, env="production", target=None):
    """
    download to users downloads or target all images for the prep print pieces key
    """
    path = f"s3://res-data-{env}/flows/v1/dxa-prep_ordered_pieces/primary/extract_parts/{key}"
    res.utils.logger.info(f"fetching {path}")
    s3 = res.connectors.load("s3")
    from pathlib import Path
    import getpass as gt

    if not target:
        user = gt.getuser()
        target = f"/Users/{user}/Downloads/{key}"
        Path(target).mkdir(exist_ok=True)

    for f in list(s3.ls(path)):
        fname = Path(f).name
        target_file = f"{target}/{fname}"
        res.utils.logger.debug(f"Downloading {f} to {target_file}")
        s3._download(f, target)

    res.utils.logger.info("done")


def try_flush_stuck(event={}, context={}, plan=False):
    """
    possibly we can run this once a day on prod
    """
    import res
    from res.connectors.airtable import AirtableConnector
    import json

    assets = AirtableConnector.get_airtable_table_for_schema_by_name(
        "make.print_assets", view="viwVreapFyS2GYTUw"
    )
    snowflake = res.connectors.load("snowflake")
    ids = [f"'{r}'" for r in list(assets["record_id"])]
    ids = ",".join(ids)
    request_q = snowflake.execute(
        f"""SELECT * FROM IAMCURIOUS_PRODUCTION.PREP_PIECES_REQUESTS_QUEUE where "id" in ({ids})   """
    )
    request_q["rc"] = request_q["RECORD_CONTENT"].map(json.loads)
    assets = list(request_q["rc"])
    kafka = res.connectors.load("kafka")
    if not plan:
        for asset in assets:
            kafka["res_meta.dxa.prep_pieces_requests"].publish(asset, use_kgateway=True)
    return assets


def rebuild_healing_from_ppp(plan=False, summary=False, existing_piece_records=None):
    """
    adding temp utility here for now
    this is a snippet that shows how we can build historic data about healed pieces using the response history which goes back as far as 3d data
    we can store the number of unique requests to heal a piece of a ONE using the unique piece code

    #todo - update this to update data since window
    """
    import res
    import json
    import pandas as pd
    from tqdm import tqdm
    from tenacity import retry, stop_after_attempt, wait_fixed

    hasura = res.connectors.load("hasura")
    existing_piece_records = existing_piece_records or []

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
    def update(records):
        Q = """mutation MyMutation($objects: [make_one_piece_healing_requests_insert_input!] = {}) {
            insert_make_one_piece_healing_requests(objects: $objects, on_conflict: {constraint: one_piece_healing_requests_pkey, update_columns: [metadata]}) {
                returning {
                id
                }
            }
            }
            """
        return hasura.execute_with_kwargs(Q, objects=records)

    def extract_pieces(row):
        if "pieces" in row and len(row["pieces"]):
            return list(row["pieces"].keys())
        if "make_pieces" in row:
            return [p["piece_name"] for p in row["make_pieces"]]

    def get_first_piece_body(r):
        if r:
            parts = r[0].split("-")[:3]
            return f"-".join(parts)

    def get_version(b):
        try:
            return int(float(b.split("-")[-1].replace("V", "")))
        except:
            pass

    def oid(d):
        return res.utils.uuid_str_from_dict(
            {"one_number": int(float(d["one_number"])), "piece_code": d["code"]}
        )

    def _id(d):
        request_id = d["request_id"]

        return res.utils.uuid_str_from_dict(
            {
                "request_id": request_id,
                "one_number": int(float(d["one_number"])),
                "piece_code": d["code"],
            }
        )

    def ppps():
        Q = f"""SELECT * FROM IAMCURIOUS_PRODUCTION.PREP_PIECES_RESPONSES_QUEUE"""
        data = res.connectors.load("snowflake").execute(Q)
        data["payloads"] = data["RECORD_CONTENT"].map(json.loads)
        data = pd.DataFrame([d for d in data["payloads"]])
        data["rank"] = data["metadata"].map(
            lambda x: x.get("rank") if isinstance(x, dict) else None
        )
        return data.drop_duplicates(subset=["id"], keep="first")

    # takes a while - could filter on server
    df = ppps()
    H = df[df["rank"] == "Healing"]
    CHK = H[
        [
            "pieces",
            "make_pieces",
            "one_number",
            "id",
            "body_code",
            "body_version",
            "created_at",
        ]
    ]
    CHK["request_id"] = CHK["id"]
    CHK["pieces_ext"] = CHK.apply(extract_pieces, axis=1)
    CHK["versioned_body_code"] = CHK["pieces_ext"].map(get_first_piece_body)
    CHK["body_version_ex"] = CHK["versioned_body_code"].map(get_version)
    CHK = CHK[CHK["body_version_ex"].notnull()]
    CHK["body_version"] = CHK["body_version_ex"].map(int)
    CHK = CHK.explode("pieces_ext").reset_index()
    CHK["piece_name"] = CHK["pieces_ext"]
    CHK["piece_code"] = CHK["pieces_ext"].map(lambda x: f"-".join(x.split("-")[-2:]))
    CHK["code"] = CHK["piece_code"]
    # this id is the legacy one created from the full piece name and airtable request id
    CHK["make_pieces_piece_id"] = CHK.apply(
        lambda a: res.utils.res_hash(f"{a['request_id']}{a['piece_name']}".encode()),
        axis=1,
    )
    CHK["id"] = CHK.apply(_id, axis=1)
    CHK["piece_id"] = CHK.apply(oid, axis=1)

    CHK = CHK[
        [
            "request_id",
            "body_version",
            "versioned_body_code",
            "code",
            "piece_name",
            "make_pieces_piece_id",
            "id",
            "piece_id",
            "one_number",
            "created_at",
        ]
    ]

    CHK = CHK[~CHK["make_pieces_piece_id"].isin(list(existing_piece_records))]

    if plan:
        return CHK

    for record in tqdm(CHK.to_dict("records")):
        d = {
            "id": record["id"],
            "request_id": record["request_id"],
            # fow now we are using a piece id based on the one but when we move away from one numbers we can change it
            "piece_id": record["piece_id"],
            "metadata": {
                "one_number": record["one_number"],
                "piece_code": record["code"],
                "versioned_body": record["versioned_body_code"],
            },
            "print_asset_piece_id": record["make_pieces_piece_id"],
            # need to test ths but important for new entries to keep an original created-at
            "created_at": record["created_at"],
        }
        r = update(d)

    print("done")

    return CHK
