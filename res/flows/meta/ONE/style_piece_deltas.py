from tqdm import tqdm
from res.media.images.outlines import get_piece_outline
from res.media.images.geometry import (
    to_geojson,
    shift_geometry_to_origin,
    from_geojson,
    LinearRing,
    Polygon,
)
import res
from res.flows.meta.ONE.queries import save_piece_image_outline, get_latest_style_pieces
import pandas as pd
from shapely.geometry import shape
from shapely.ops import polygonize
from res.flows.meta.ONE.queries import (
    get_latest_style_pieces,
    get_latest_style_pieces_by_id,
    get_style_id_lookup,
)
import math
from res.utils import logger
from res.flows.meta.ONE.style_node import MetaOneNode
from res.connectors.airtable import AirtableConnector
import traceback
from res.flows.meta.bodies import get_body_asset_status


def test_should_run(row):
    if row["Body Version"] != row["current_body_version"]:
        res.utils.logger.debug(f"***there is a newer body version***")
        return False
    if pd.isnull(row["Body File Uploaded At"]):
        res.utils.logger.debug(f"***no uploaded body file***")
        return False
    if pd.isnull(row["Style Test Started At"]):
        return True

    if row["Body File Uploaded At"] > row["Style Test Started At"]:
        return True

    if (
        (res.utils.dates.utc_now() - row["Style Test Started At"]).total_seconds() / 60
    ) > 20:
        return True
    res.utils.logger.debug(
        "***The time since uploaded file + since test start failed the condition***"
    )
    return False


def request_style_tests_for_body(body_code, body_version, style_codes=None):
    from res.flows import FlowEventProcessor

    fp = FlowEventProcessor()
    event = fp.make_sample_flow_payload_for_function("meta.ONE.style_piece_deltas")
    from res.flows.meta.ONE.style_piece_deltas import generator, handler

    event["assets"] = [
        {
            "body_code": body_code,
            "body_version": body_version,
            "style_code_filter": style_codes,
        }
    ]

    key = f"{body_code}-{body_version}-style-test-{res.utils.res_hash()}".lower()
    if style_codes and isinstance(style_codes, str):
        key = (
            f"{style_codes.replace(' ','-')}-v{body_version}-style-test-single".lower()
        )
    argo = res.connectors.load("argo")
    argo.handle_event(event, unique_job_name=key)

    res.utils.logger.info(f"submitted argo {key} ")


def notify_body_started(assets):
    tab = res.connectors.load("airtable")["appa7Sw0ML47cA8D1"]["tbl0ZcEIUzSLCGG64"]
    keys = []
    for asset in assets:
        body_code = asset["body_code"]
        body_version = asset["body_version"]
        key = f"{body_code}-V{body_version}"
        keys.append(key)

    res.utils.logger.info(f"observing test started for  keys {keys}")

    pred = AirtableConnector.make_key_lookup_predicate(keys, "Name")

    data = tab.to_dataframe(
        fields=[
            "Name",
            "Style Test Started At",
            "Body File Uploaded At",
            "Body Code",
            "Body Version",
        ],
        filters=pred,
    )

    data["current_body_version"] = data["Body Code"].map(
        lambda b: get_body_asset_status(b)["patternVersionNumber"]
    )

    status = True

    for c in ["Style Test Started At", "Body File Uploaded At"]:
        if c not in data.columns:
            data[c] = None
        data[c] = pd.to_datetime(data[c])
    if len(data) == 0:
        return False

    # run a test to see if the body that is requested should actually spawn a job against styles
    data["has_start"] = data["Style Test Started At"].notnull()
    data["should_run"] = data.apply(test_should_run, axis=1)
    ok_data = data[data["should_run"]]
    if len(ok_data) == 0:
        status = False

    res.utils.logger.info(f"The test should run and notify start: {status}")

    # we wanna make sure we always leave some start date regardless of the test but only set a new one if we are running a test
    if status == True:
        data["Style Test Started At"] = res.utils.dates.utc_now_iso_string()
    else:
        data["Style Test Started At"] = data["Style Test Started At"].fillna(
            res.utils.dates.utc_now_iso_string()
        )

    for record in data[["record_id", "Style Test Started At"]].to_dict("records"):
        tab.update_record(record)

    return status


def generator(event, context={}, compress=True):
    """
    Pass in either a body or a list of skus in the apply color queue
    TODO - for now just do sku set mode and we can figure out the list on client..
    handler will process latest pieces for each
    """

    def break_into_batches(original_list, num_batches=10):
        batch_size = len(original_list) // num_batches
        batches = [
            original_list[i : i + batch_size]
            for i in range(0, len(original_list), batch_size)
        ]

        return batches

    with res.flows.FlowContext(event, context) as fc:
        if len(fc.assets):
            res.utils.logger.info(
                f"Assets supplied by payload - using instead of kafka"
            )
            all_styles = []
            for asset in fc.assets:
                body_code = asset["body_code"]
                body_version = asset["body_version"]
                res.utils.logger.info(f"request styles for {body_code}-V{body_version}")
                all_styles += get_styles_for_body_and_version(
                    body_code=body_code,
                    body_version=body_version,
                    style_codes=asset.get("style_code_filter"),
                    # if we test for specific styles, we can check even for same body
                    # but for the bach we should only do the older styles and assume the existing styles on VSame are fine
                    ignore_styles_on_current_version=asset.get("style_code_filter")
                    is None,
                )
            res.utils.logger.info(f"returning {len(all_styles)} styles")

            # only notify the body if we are not filtering for a style
            if not asset.get("style_code_filter"):
                if not notify_body_started(assets=fc.assets):
                    # NB: this only works if we are assuming its one body per request
                    # otherwise we should check which bodies we could notify
                    res.utils.logger.warn(
                        f"We could not notify the start of test or a recent test already exists - skipping"
                    )
                    return []

            if len(all_styles) > 0:
                res.utils.logger.info("looking up styles map...")

                lu_styles = get_style_id_lookup(
                    specific_style_code=asset.get("style_code_filter")
                )
                # add the style ids to the payloads for faster lookup
                for s in all_styles:
                    s["style_id"] = lu_styles.get(s["Style Code"])

                # if we have too many we should batch them
                if len(all_styles) > 10:
                    batches_all_styles = break_into_batches(all_styles)
                    all_styles = []
                    for b in batches_all_styles:
                        sample = fc.asset_list_to_payload(b[:1], compress=compress)[0]
                        sample["assets"] = b
                        all_styles.append(sample)
                else:
                    all_styles = fc.asset_list_to_payload(all_styles, compress=compress)

            return all_styles


@res.flows.flow_node_attributes(memory="50Gi", disk="5G")
def handler(event, context={}):
    s3 = res.connectors.load("s3")
    with res.flows.FlowContext(event, context) as fc:
        for payload in fc.decompressed_assets:
            res.utils.logger.info(payload)
            body_code = payload["body_code"]
            body_version = payload["body_version"]

            res.utils.logger.info(f"proc payload {payload}")
            try:
                check_styles_for_insignificant_body_change(
                    style_row=payload, body_code=body_code, body_version=body_version
                )
            except Exception as ex:
                res.utils.logger.warn(f"**********Failed on {payload}")
                res.utils.logger.warn(traceback.format_exc())
                res.utils.logger.warn(f"*************************")
    return {}


def reducer(event, context={}):
    """
    as we run the test we should update on the body response that usually triggers it
    """

    try:
        with res.flows.FlowContext(event, context) as fc:
            tab = res.connectors.load("airtable")["appa7Sw0ML47cA8D1"][
                "tbl0ZcEIUzSLCGG64"
            ]
            keys = []
            for asset in fc.assets:
                body_code = asset["body_code"]
                body_version = asset["body_version"]
                key = f"{body_code}-V{body_version}"
                keys.append(key)

            res.utils.logger.info(f"observing test completed for  keys {keys}")

            pred = AirtableConnector.make_key_lookup_predicate(keys, "Name")

            data = tab.to_dataframe(
                fields=["Name", "Style Test Finished At"], filters=pred
            )
            data["Style Test Finished At"] = res.utils.dates.utc_now_iso_string()
            for record in data[["record_id", "Style Test Finished At"]].to_dict(
                "records"
            ):
                tab.update_record(record)
    except:
        res.utils.logger.warn(f"{traceback.format_exc()}")

    return {}


def get_styles_for_body_and_version(
    body_code, body_version, style_codes=None, ignore_styles_on_current_version=False
):
    airtable = res.connectors.load("airtable")
    redis = res.connectors.load("redis")
    cache = redis["CACHE"]["META"]
    res.utils.logger.debug(f"Loading materials...")
    # get the materials as we may need to apply a buffer to compare like with like
    cache["MATERIALS"] = dict(
        airtable["app1FBXxTRoicCW8k"]["tblD1kPG5jpf6GCQl"]
        .to_dataframe(fields=["Material Code", "Offset Size (Inches)"])[
            ["Material Code", "Offset Size (Inches)"]
        ]
        .dropna()
        .values
    )

    res.utils.logger.debug(f"Loading ACQ styles...")
    # get all the successful apply color requests for this body and version
    apply_color_requests = airtable["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]
    filters = f"""AND(SEARCH("{body_code}",{{Body Code}}),{{Apply Color Flow Status}}="Done")"""

    fields = [
        "_record_id",
        "Request Key",
        "Style Code",
        "Request Auto Number",
        "Body Version",
    ]

    completed_styles = apply_color_requests.to_dataframe(filters=filters, fields=fields)

    # this is a lazy post filter
    if style_codes:
        if not isinstance(style_codes, list):
            style_codes = [style_codes]
        completed_styles = completed_styles[
            completed_styles["Style Code"].isin(style_codes)
        ]

    if not len(completed_styles):
        return []

    if "Body Version" not in completed_styles.columns:
        completed_styles["Body Version"] = None

    # get all the styles that are done but keep the latest body version
    completed_styles = (
        completed_styles.sort_values("Body Version").drop_duplicates(
            subset=["Style Code"], keep="last"
        )
    ).rename(columns={"Body Version": "Style Body Version"})
    completed_styles["body_code"] = body_code
    # this is the target version
    completed_styles["body_version"] = body_version

    if ignore_styles_on_current_version:
        l = len(completed_styles)
        completed_styles["Style Body Version"] = (
            completed_styles["Style Body Version"].fillna(0).map(int)
        )
        completed_styles = completed_styles[
            completed_styles["Style Body Version"] < body_version
        ]
        res.utils.logger.info(
            f"filtering styles that are on the older body version only. {len(completed_styles)} out of {l}"
        )

        if not len(completed_styles):
            return []

    return completed_styles[
        ["Style Code", "Style Body Version", "body_code", "body_version"]
    ].to_dict("records")


def check_styles_for_insignificant_body_change(
    body_code,
    body_version,
    style_row,
    # change tolerance from 0.01 to 0.02 after testing same version diffs
    rel_tol=0.02,
):
    """
    for a particular body transition check the previous style (how far back to go - prob one and we can manually bump others)
    Note that the style row has its own body version
    """

    logger.info(f"*** copy_style_if_insignificant_body_change for {body_code} ***")

    """
    Given a body and version, copy the styles for that body and version to the next version
    IF the body change is insignificant (i.e. the body geometries fit within the piece outlines)
    """

    hasura = res.connectors.load("hasura")
    s3 = res.connectors.load("s3")
    material_offset_cache = res.connectors.load("redis")["CACHE"]["META"]["MATERIALS"]

    # get the relevant BW
    snake_case_body_code = body_code.lower().replace("-", "_")
    browzwear_path = f"s3://meta-one-assets-prod/bodies/3d_body_files/{snake_case_body_code}/v{body_version}/{body_code}-V{body_version}-3D_BODY.bw"
    bw_file_updated = None
    browzwear_file = list(s3.ls_info(browzwear_path))
    if browzwear_file:
        bw_file_updated = browzwear_file[0]["last_modified"]
        browzwear_file = browzwear_file[0]["path"]
    else:
        raise Exception(f"There is no browzware file at {browzwear_path}")
    # get the exterior geometries for this body and curr_version
    GET_BODY_PIECES = """
        query getBodyPieces($body_code: String!, $version: numeric!) {
            meta_bodies(where: {body_code: {_eq: $body_code}, version: {_eq: $version}, body_pieces: {deleted_at: {_is_null: true}}}) {
                id
                body_pieces {
                    id
                    piece_key
                    size_code
                    outer_geojson
                    updated_at
                }
            }
       }

    """
    res.utils.logger.debug(f"Loading pieces...")
    result = hasura.execute_with_kwargs(
        GET_BODY_PIECES, body_code=body_code, version=body_version
    )
    size_piece_shape = {}
    size_piece_updates = {}

    compound = lambda a, b: f"{a}_{b}"

    for piece in result["meta_bodies"][0]["body_pieces"]:
        updated_at = piece["updated_at"]
        outline_data = piece["outer_geojson"]
        geojson = from_geojson(piece["outer_geojson"])
        polygon = list(polygonize(shape(geojson)))[0]
        # im not sure why it seems necessary to trim the piece keys they are wrong with _P at the end sometimes
        pkey = piece["piece_key"].split("_")[0]
        key = compound(pkey, piece["size_code"])
        size_piece_shape[key] = polygon
        size_piece_updates[key] = updated_at

    style_code = style_row["Style Code"]
    style_id = style_row.get("style_id")
    logger.info(
        f"Checking {style_code} pieces - the ACQ body version is {style_row['Style Body Version']}. Style id {style_id}"
    )

    # get the style pieces
    style_pieces = get_latest_style_pieces(
        style_id,
        cache_if_missing=True,
        check_specific_body_version=style_row["Style Body Version"],
    )
    # if there are going to be no pieces then we have a problem
    significant_change = False if len(style_pieces) else True
    # if there are any body pieces not in the style pieces or vice versa then we have a problem
    remove_body_code_version = lambda x: "-".join(x.split("-")[3:])
    style_piece_keys = style_pieces["body_piece_key"].apply(remove_body_code_version)
    body_piece_keys = [remove_body_code_version(k) for k in size_piece_shape.keys()]
    significant_change = set(style_piece_keys) != set(body_piece_keys)
    for j, piece_row in style_pieces.iterrows():
        # this is the compound key for the piece with size code suffix
        body_piece_key = piece_row["body_piece_key"]
        outline_data = piece_row["base_image_outline"]
        if str(outline_data)[:5] == "s3://":
            style_piece_polygon = Polygon(s3.read(outline_data).values)
        else:
            geojson = from_geojson(outline_data)
            style_piece_polygon = list(polygonize(shape(geojson)))[0]

        style_piece_polygon = shift_geometry_to_origin(style_piece_polygon)
        piece_material = piece_row["material_code"]
        body_version_part = body_piece_key.split("-")[2]

        if pd.isnull(style_row["Style Body Version"]):
            style_row["Style Body Version"] = int(
                body_version_part.lower().replace("v", "")
            )
            res.utils.logger.warn(
                f"<<<<<<<<<<<< we needed to infer the style version {style_row['Style Body Version']} from the style pieces because its not in ACQ for this style"
            )
        body_piece_polygon = size_piece_shape.get(
            body_piece_key.replace(body_version_part, f"V{body_version}")
        )
        if not body_piece_polygon:
            logger.warn(
                f"Could not find body piece {body_piece_key}(V{body_version}) in {size_piece_shape.keys()}"
            )
            significant_change = True
            break

        buffer_inches = material_offset_cache.get(piece_material) or 0

        buffer_dots = buffer_inches * 300 if pd.notnull(buffer_inches) else 0

        body_piece_polygon = body_piece_polygon.buffer(buffer_dots, join_style=2)
        body_piece_polygon = shift_geometry_to_origin(body_piece_polygon)

        # check that the overlap is within the tolerance
        intersection = body_piece_polygon.intersection(style_piece_polygon)
        body_piece_area = body_piece_polygon.area
        style_piece_area = style_piece_polygon.area
        intersection_area = intersection.area

        if not math.isclose(
            body_piece_area,
            intersection_area,
            rel_tol=rel_tol,
        ) or not math.isclose(
            style_piece_area,
            intersection_area,
            rel_tol=rel_tol,
        ):
            logger.info(
                f"Significant change for {style_code} {body_piece_key} {intersection_area} {body_piece_area} {style_piece_area}"
            )
            significant_change = True
            # return (intersection, body_piece_polygon, style_piece_polygon)
            break
    """
    if the styles have a significant change or not we set the status but always update the style that we checked
    """
    res.utils.logger.info(f"Are changes detected on {style_code}: {significant_change}")

    MetaOneNode.update_style_status_for_piece_delta_test(
        style_code,
        style_body_version=style_row["Style Body Version"],
        significant_change=significant_change,
        bw_file_uri=browzwear_file,
        bw_file_updated=bw_file_updated.isoformat(),
        bw_file_body_version=body_version,
    )

    res.utils.logger.info("<<<<<DONE>>>>>")


def do_one(event, context={}):
    """
    a slow back fill e.g. every 30 mins
    """
    with res.flows.FlowContext(event, context) as fc:
        airtable = res.connectors.load("airtable")
        df = airtable.get_table_data(
            "appa7Sw0ML47cA8D1/tbl0ZcEIUzSLCGG64",
            fields=[
                "Name",
                "Last Modified At",
                "Style Test Finished At",
                "Style Test Started At",
                "Body File Uploaded At",
                "Contracts Failed Codes",
            ],
        )
        df = df[df["Name"].notnull()]
        df = df[df["Name"].map(lambda x: len(x.split("-")) == 3)]
        df["body_code"] = df["Name"].map(lambda x: "-".join(x.split("-")[:2]))
        df["body_version"] = (
            df["Name"].map(lambda x: x.split("-")[-1].replace("V", "")).map(int)
        )

        # only consider after V1
        df = df[df["body_version"] > 1]

        # only consider only no contract vars
        df = df[df["Contracts Failed Codes"].isnull()]

        # sorting by any body files uploaded to the system
        records = df[
            # if we ever tested or we tested before the body was re-uploaded - redo
            (
                (df["Style Test Started At"].isnull())
                | (df["Body File Uploaded At"] > df["Style Test Started At"])
            )
            & (df["Body File Uploaded At"].notnull())
        ].sort_values("Body File Uploaded At")

        # bottom two
        for record in records.to_dict("records")[-2:]:
            request_style_tests_for_body(
                record["body_code"], body_version=int(record["body_version"])
            )
