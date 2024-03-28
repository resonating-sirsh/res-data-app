"""
Adding some snippets for writing up later
"""

import pytest
import res
import os

data = {
    "body_code": "kt_2011",
    "flow": "3d",
    "body_version": "v0",
    "path": "s3://res-data-platform/samples/zip_files/simple.zip",
}


@pytest.mark.service
def test_harness_service_requst_body(data):
    from res.flows.meta.bodies import get_body_request_payloads

    body_code = data["body_code"]
    body_code = body_code.upper().replace("_", "-")
    flow = data["flow"]
    default_size_only = data.get("default_size_only", False)
    path = data.get("path")
    body_version = data.get("body_version")
    skip_extract = data.get("skip_extract", True)

    def get_body_meta_extraction_path(body, version, path, flow, suffix="extracted"):

        if "3d" in flow:
            filename = path.split("/")[-1].split(".")[0]
            root = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body}/{version}/{suffix}/{filename}"
            res.utils.logger.info(f"Extracting to {root}")
            return root

        return ""

    if path[-4:] == ".zip":
        # an example extract content is
        #  s3://meta-one-assets-prod/bodies/3d_body_files/dj_6000/v1/extracted/dxf_by_size/P-2X/DJ-6000-V1-3D_BODY.dxf
        # the meta one will look for things under the body body `/extracted` but the "dxf_by_size" comes from the client pack structure
        xpath = get_body_meta_extraction_path(body_code, body_version, path, flow)
    #     if not skip_extract:
    #         s3.unzip(path, xpath)

    # assume length for legal - some of a's attributes are filtered from the kafka payload and recomputed in the meta one node...
    a = list(
        get_body_request_payloads(
            body_code,
            flow=flow,
            default_size_only=default_size_only,
            default_color=data.get("default_color"),
        )
    )

    # res.utils.logger.debug(f"Publishing asset {a}")

    # kafka[TOPIC].publish(a, use_kgateway=True, coerce=True)
    return a


# when checking the is placed after we can look at the constellation of placements
# from geopandas import GeoDataFrame
# func = p._relative_location["edge_order"]
# prj = rec.get("placement_line").project(rec['placement_position'])
# p2 = rec['placement_line'].interpolate(prj)
# mp = rec['placement_line'].interpolate(0.5, normalized=True)
# print(func(p2),func(mp), func(p2) > func(mp))
# GeoDataFrame([rec['placement_position'], rec['placement_line'], rec['notch_line'], p2, mp],columns=['geometry']).plot()

# we are checking that the flip is true on the symbol when its placed after
# test = pd.DataFrame(p._annotations)[['flip', 'operation', 'dx', 'dy', 'E', 'edge_order', 'placement_position', 'placement_line', 'notch_line']]
# rec = test.iloc[14]
# rec

##test construction placement lines
# from res.media.images.providers.dxf import SMALL_SEAM_SIZE, HALF_SMALL_SEAM_SIZE
# from geopandas import GeoDataFrame
# from matplotlib import pyplot as plt
# PLINE = SMALL_SEAM_SIZE * 0.75
# DISC = SMALL_SEAM_SIZE
# nl = rec['notch_line']
# l = rec['isolated_edge_offset']
# b = nl.interpolate(PLINE).buffer(DISC)
# I = l.intersection(b)
# pts = [I.interpolate(0), I.interpolate(I.length)]
# g= [rec['geometry'], l, nl, I.intersection(b).buffer(2) ] +  pts
# ax = GeoDataFrame(g,columns=['geometry']).plot(figsize=(20,20))
# plt.axis('off')
# plt.gca().invert_yaxis()

# test parse sectional edges for sew interface - basically just need to assign ops to the correct edges/notches

# there area  bunch of things we need to test for notches - we should ahve all SAs and adjacent SAs at corners after corner removal
# p._piece_model[2].notch_seam_corner_allowance({'edge-ref': 'next', 'operation': 'edge_stitch_fold_notch', 'position': 'near_edge_corner', 'dx': 75.0, 'dx-ref': 0.5, 'edge_id': 2, 'notch_id': 0, 'E': 1, 'hash': 'res11F77C11D8'})


# learnings:
# when the VS export does not match the body - we can have issues
# when sew does not match the body we can have issues
# in both cases we want to process a meta one with a single bundle


# when there are internal line models we show otherwise we default them to off. however need input from 3d to know how to render them
# today we have hard coded some
# to view them...
# dxf_plotting.plot_numbered_internal_lines(p._row)

# tolerance for being inside the seam allowance (see notch from call) is 37.5 -> maybe we need to make sure there is a nearby notch though


# experimental tolerance for notch removal : 35, 5 as parameters
# rational is that if we are within the seam allowance based on some tolerance and there is a nearby notch maybe just inside or outside the seam allowance then we can remove the inner notch
# because (a) there be should no notch inside the SA and we are ensuring that there is at least one there already
#  df["fail"] = (
#                 df["to_corner_distance"] + (factor - epsilon)
#                 < df["my_min_corner_threshold"]
#             ) & (df["dist_other"] < (factor + 2 * epsilon))


@pytest.mark.service
def test_unpack_asset_bundle():
    os.environ["GRAPHQL_API_URL"] = "https://api.resmagic.io/graphql"
    os.environ["RES_ENV"] = "production"
    from res.flows.meta.body.unpack_asset_bundle.unpack_asset_bundle import handler

    # roller rabbit placeholders, manually changed asset_bundle as there's no VS license at the moment
    # event = {
    #     "record_id": "recYuWXPPsaqidNqU",
    #     "body_code": "RR-3005",
    #     "body_version": "2",
    #     "asset_bundle_uri": "s3://res-temp-public-bucket/style_assets_dev/849d4d3a-83dd-4ef1-b632-55dd7879225b/asset_bundle_jl.zip",
    #     # to undo jik
    #     # "asset_bundle_uri": "s3://res-temp-public-bucket/style_assets_dev/849d4d3a-83dd-4ef1-b632-55dd7879225b/asset_bundle.zip",
    # }

    # event = {
    #     "record_id": "recacezHBEOIT1YcT",
    #     "body_code": "TT-3044",
    #     "body_version": "1",
    #     "asset_bundle_uri": "s3://res-temp-public-bucket/style_assets_dev/fa0809a3-9508-4789-a9d7-9de77289588d/asset_bundle.zip",
    # }

    # try:
    #     handler(event)
    # except Exception as e:
    #     print(e)
    #     raise e


@pytest.mark.service
def test_apply_color_request():
    os.environ["GRAPHQL_API_URL"] = "https://api.resmagic.io/graphql"
    os.environ["RES_ENV"] = "development"
    from res.flows.dxa.event_handler import handle
    from res.connectors.graphql import hasura
    from res.flows.dxa import node_run

    requestor_id = "recQkTKbFXALlSRz3"
    event = {
        "event_type": hasura.ChangeDataTypes.CREATE.value,
        "status": node_run.Status.IN_PROGRESS,
        "requestor_id": requestor_id,
        "job_type": node_run.FlowNodes.EXPORT_STYLE_BUNDLE.value,
    }

    handle(event)


@pytest.mark.service
def test_generate_meta_one_sequence():
    os.environ["GRAPHQL_API_URL"] = "https://api.resmagic.io/graphql"
    os.environ["RES_ENV"] = "development"
    from res.flows.meta.one_marker.generate_meta_one.generate_meta_one import (
        start_generate_meta_one,
        unpack_pieces_and_sort_by_size,
        unpack_turntable,
        extract_images_from_pdf_pieces,
        run_pieces_checks,
    )

    try:
        event = {"record_id": "rec0zxX2OPBJRMHwk"}
        # e = unpack_turntable(event)

        event = start_generate_meta_one(event, debugging=True)
        print("from start_generate_meta_one", event)

        events = unpack_pieces_and_sort_by_size(event, debugging=True)
        event = events[0]
        print("from unpack_pieces_and_sort_by_size", event)

        event = extract_images_from_pdf_pieces(event)
        print("from extract_images_from_pdf_pieces", event)

        event = run_pieces_checks(event)
        print("from run_pieces_checks", event)
    except Exception as e:
        import traceback

        print(traceback.format_exc())
        print(e)


@pytest.mark.service
def test_generate_meta_one():
    os.environ["GRAPHQL_API_URL"] = "https://api.resmagic.io/graphql"
    os.environ["RES_ENV"] = "development"
    from res.flows.meta.one_marker.generate_meta_one.generate_meta_one import (
        start_generate_meta_one,
        unpack_pieces_and_sort_by_size,
        unpack_turntable,
        extract_images_from_pdf_pieces,
        run_pieces_checks,
    )

    try:
        # event = {
        #     "record_id": "recJ0A3GRAvYyeozx",
        # }
        # # # # # e = unpack_turntable(event)
        # event = start_generate_meta_one(event)
        # event = {
        #     "record_id": "recJ0A3GRAvYyeozx",
        #     "body_code": "KT-3008",
        #     "body_version": 7,
        #     "color_code": "LIGHET",
        #     "material_code": "CTNSP",
        #     "zipped_raw_pieces_uri": "s3://res-temp-public-bucket/style_assets_dev/94fb03f8-ebc4-41b8-b736-18063e48fd32/asset_bundle.zip",
        #     "piece_material_offset_lookup": {
        #         "TOPFTPNL-S": 300,
        #         "TOPBKPNL-S": 300,
        #         "TOPSLPNLLF-S": 300,
        #         "TOPSLPNLRT-S": 300,
        #         "TOPHMBND-S": 300,
        #         "TOPSLCUFLF-S": 300,
        #         "TOPSLCUFRT-S": 300,
        #         "TOPNKBND-S": 300,
        #         "TOPNKBDG-C": 450,
        #     },
        #     "piece_material_code_lookup": {
        #         "TOPFTPNL-S": "CTNSP",
        #         "TOPBKPNL-S": "CTNSP",
        #         "TOPSLPNLLF-S": "CTNSP",
        #         "TOPSLPNLRT-S": "CTNSP",
        #         "TOPHMBND-S": "CTNSP",
        #         "TOPSLCUFLF-S": "CTNSP",
        #         "TOPSLCUFRT-S": "CTNSP",
        #         "TOPNKBND-S": "CTNSP",
        #         "TOPNKBDG-C": "CTNBA",
        #     },
        # }

        # a = unpack_pieces_and_sort_by_size(event)

        # event = {
        #     "request_id": "recJ0A3GRAvYyeozx",
        #     "body_code": "KT-3008",
        #     "body_version": 7,
        #     "color_code": "LIGHET",
        #     "material_code": "CTNSP",
        #     "size_code": "3ZZMD",
        #     "dxf_file_path": "s3://meta-one-assets-prod/color_on_shape/kt_3008/v7/lighet/dxf/KT-3008-V7-3D_BODY.dxf",
        #     "size_pieces_path": "color_on_shape/kt_3008/v7/lighet/3zzmd",
        #     "raw_pieces_paths": [
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPSLPNLRT-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPSLCUFRT-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPSLPNLLF-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPHMBND-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPBKPNL-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPFTPNL-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPSLCUFLF-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/450/KT-3008-V7-TOPNKBDG-C.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPNKBND-S.pdf",
        #     ],
        #     "piece_material_offset_lookup": {
        #         "TOPFTPNL-S": 300,
        #         "TOPBKPNL-S": 300,
        #         "TOPSLPNLLF-S": 300,
        #         "TOPSLPNLRT-S": 300,
        #         "TOPHMBND-S": 300,
        #         "TOPSLCUFLF-S": 300,
        #         "TOPSLCUFRT-S": 300,
        #         "TOPNKBND-S": 300,
        #         "TOPNKBDG-C": 450,
        #     },
        #     "piece_material_code_lookup": {
        #         "TOPFTPNL-S": "CTNSP",
        #         "TOPBKPNL-S": "CTNSP",
        #         "TOPSLPNLLF-S": "CTNSP",
        #         "TOPSLPNLRT-S": "CTNSP",
        #         "TOPHMBND-S": "CTNSP",
        #         "TOPSLCUFLF-S": "CTNSP",
        #         "TOPSLCUFRT-S": "CTNSP",
        #         "TOPNKBND-S": "CTNSP",
        #         "TOPNKBDG-C": "CTNBA",
        #     },
        # }

        # event = extract_images_from_pdf_pieces(event)

        # event = {
        #     "request_id": "recJ0A3GRAvYyeozx",
        #     "body_code": "KT-3008",
        #     "body_version": 7,
        #     "color_code": "LIGHET",
        #     "material_code": "CTNSP",
        #     "size_code": "3ZZMD",
        #     "dxf_file_path": "s3://meta-one-assets-prod/color_on_shape/kt_3008/v7/lighet/dxf/KT-3008-V7-3D_BODY.dxf",
        #     "size_pieces_path": "color_on_shape/kt_3008/v7/lighet/3zzmd",
        #     "raw_pieces_paths": [
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPSLPNLRT-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPSLCUFRT-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPSLPNLLF-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPHMBND-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPBKPNL-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPFTPNL-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPSLCUFLF-S.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/450/KT-3008-V7-TOPNKBDG-C.pdf",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/raw_pieces/300/KT-3008-V7-TOPNKBND-S.pdf",
        #     ],
        #     "piece_material_offset_lookup": {
        #         "TOPFTPNL-S": 300,
        #         "TOPBKPNL-S": 300,
        #         "TOPSLPNLLF-S": 300,
        #         "TOPSLPNLRT-S": 300,
        #         "TOPHMBND-S": 300,
        #         "TOPSLCUFLF-S": 300,
        #         "TOPSLCUFRT-S": 300,
        #         "TOPNKBND-S": 300,
        #         "TOPNKBDG-C": 450,
        #     },
        #     "piece_material_code_lookup": {
        #         "TOPFTPNL-S": "CTNSP",
        #         "TOPBKPNL-S": "CTNSP",
        #         "TOPSLPNLLF-S": "CTNSP",
        #         "TOPSLPNLRT-S": "CTNSP",
        #         "TOPHMBND-S": "CTNSP",
        #         "TOPSLCUFLF-S": "CTNSP",
        #         "TOPSLCUFRT-S": "CTNSP",
        #         "TOPNKBND-S": "CTNSP",
        #         "TOPNKBDG-C": "CTNBA",
        #     },
        #     "png_pieces_paths": [
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/pieces/300/KT-3008-V7-TOPSLPNLRT-S.png",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/pieces/300/KT-3008-V7-TOPSLCUFRT-S.png",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/pieces/300/KT-3008-V7-TOPSLPNLLF-S.png",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/pieces/300/KT-3008-V7-TOPHMBND-S.png",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/pieces/300/KT-3008-V7-TOPBKPNL-S.png",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/pieces/300/KT-3008-V7-TOPFTPNL-S.png",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/pieces/300/KT-3008-V7-TOPSLCUFLF-S.png",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/pieces/450/KT-3008-V7-TOPNKBDG-C.png",
        #         "color_on_shape/kt_3008/v7/lighet/3zzmd/pieces/300/KT-3008-V7-TOPNKBND-S.png",
        #     ],
        #     "total_pieces_count": 9,
        # }

        # run_pieces_checks(event)

        pass
    except Exception as e:
        print(e)


@pytest.mark.service
def test_meta_one_preview():
    os.environ["RES_ENV"] = "production"

    from res.flows.meta.marker import try_generate_meta_one_preview

    # a = {
    #     "id": "recTwM3vxK7H0nWGl",
    #     "unit_key": "KT1012 CT406 SELF",
    #     "body_code": "KT-1012",
    #     "body_version": 6,
    #     "color_code": "SELF--",
    #     "size_code": "5ZZXL",
    #     "normed_size": "XL",
    #     "dxf_path": "s3://meta-one-assets-prod/color_on_shape/kt_1012/v6/self--/dxf/KT-1012-V6-3D_BODY.dxf",
    #     "pieces_path": "s3://meta-one-assets-prod/color_on_shape/kt_1012/v6/self--/5zzxl/pieces",
    #     "meta_one_path": "s3://meta-one-assets-prod/styles/meta-one/3d/kt_1012/v6_16fd1af030/self--/5zzxl",
    #     "piece_material_mapping": {"default": "CT406"},
    #     "piece_color_mapping": {"default": "SELF--"},
    #     "material_properties": {
    #         "CT406": {
    #             "compensation_width": 1.1111112,
    #             "paper_marker_compensation_length": 1,
    #             "order_key": None,
    #             "compensation_length": 1.0526316,
    #             "material_stability": "Stable",
    #             "paper_marker_compensation_width": 1,
    #             "pretreatment_type": "Cotton",
    #             "offset_size": None,
    #             "fabric_type": "Woven",
    #             "cuttable_width": 60,
    #         }
    #     },
    #     "status": "ENTERED",
    #     "style_name": " Cargo Skirt - White in Stretch Organic Cotton Twill",
    #     "tags": [],
    #     "flow": "3d",
    #     "unit_type": "RES_STYLE",
    #     "created_at": "2022-12-27T11:30:26.578067+00:00",
    # }

    # try_generate_meta_one_preview(a)


@pytest.mark.service
def test_export_meta_one():
    os.environ["RES_ENV"] = "development"

    from res.flows.meta.marker import _decorate_assets, export_meta_one

    # a = {
    #     "id": "recIEqnkE8d4jrbCz",
    #     "unit_key": "KT3030 LTCSL BLACSF",
    #     "body_code": "KT-3030",
    #     "body_version": 5,
    #     "color_code": "BLACSF",
    #     "size_code": "3ZZMD",
    #     "sample_size": "null",
    #     "dxf_path": "s3://meta-one-assets-prod/color_on_shape/kt_3030/v5/blacsf/dxf/KT-3030-V5-BLACSF.dxf",
    #     "pieces_path": "s3://meta-one-assets-prod/color_on_shape/kt_3030/v5/blacsf/3zzmd/pieces",
    #     "piece_material_mapping": {
    #         "SHTSLUNPRT-S": "LTCSL",
    #         "SHTSLPNLRT-S": "LTCSL",
    #         "default": "LTCSL",
    #         "SHTFTPKTLF-S": "LTCSL",
    #         "SHTSLCUFRT-BF": "LTCSL",
    #         "SHTSLPNLLF-S": "LTCSL",
    #         "SHTSLCUFLF-BF": "LTCSL",
    #         "SHTNKCLRTP-BF": "LTCSL",
    #         "SHTNKCLRUN-BF": "LTCSL",
    #         "SHTSLUNPLF-S": "LTCSL",
    #         "SHTNKCLSTP-BF": "LTCSL",
    #         "SHTNKCLSUN-BF": "LTCSL",
    #     },
    #     "piece_color_mapping": {
    #         "SHTSLUNPRT-S": "BLACQA",
    #         "SHTSLPNLRT-S": "BLACXY",
    #         "default": "BLACSF",
    #         "SHTFTPKTLF-S": "BLACXY",
    #         "SHTSLCUFRT-BF": "BLACQA",
    #         "SHTSLPNLLF-S": "BLACXY",
    #         "SHTSLCUFLF-BF": "BLACQA",
    #         "SHTNKCLRTP-BF": "BLACXY",
    #         "SHTNKCLRUN-BF": "BLACXY",
    #         "SHTSLUNPLF-S": "BLACQA",
    #         "SHTNKCLSTP-BF": "BLACXY",
    #         "SHTNKCLSUN-BF": "BLACXY",
    #     },
    #     "status": "ENTERED",
    #     "tags": [],
    #     "flow": "3d",
    #     "unit_type": "RES_STYLE",
    #     "created_at": "2022-12-30T14:29:12.884847+00:00",
    # }

    # a = _decorate_assets([a])[0]

    # export_meta_one(a)
