import res
import pandas as pd
from res.media.images.geometry import outlines_from_point_set_groups
from res.media.images.providers import DxfFile
from res.media.images import outlines
from res.utils import logger
from res.flows.meta.marker import export_meta_one
import json
from datetime import datetime, timedelta
from res.flows.meta.utils import parse_status_details_for_issues

BODIES_S3_ROOT = "meta-one-assets-prod/bodies"
DXF_MARKER_PIECE_TYPES = ["self", "block_fuse"]

"""
adding some aspects of body onboarding for meta
these are pre style assets 
we need to understand how to map sizes, materials and colors to body creation
this can be used to create a meta one on a 2d or 3d asset creation flow
it is important because we can pre validate body assets before creating styles
"""


def get_body_preferred_extracted_sizes(body_code, body_version):
    """
    of all the dxf data extracted on a conventional path, check for change in size scale
    using info about aliases mapped to an acc-size, keep only the latest extracted sizes
    """
    Q = """query body($code: String){
        body(number:$code){
        name
        code
        availableSizes{
            code
            sizeNormalized
            aliases
        }
    }
    }"""

    try:
        g = res.connectors.load("graphql")
        r = g.query_with_kwargs(Q, code=body_code)["data"]["body"]["availableSizes"]
        size_alias_map = dict(
            pd.DataFrame(r).explode("aliases")[["aliases", "code"]].values
        )

        body_code = body_code.lower().replace("-", "_")
        s3 = res.connectors.load("s3")
        path = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_code}/v{body_version}/extracted/dxf_by_size"
        data = pd.DataFrame(list(s3.ls_info(path)))
        data["size"] = data["path"].map(lambda x: x.split("/")[-2])
        data["size_code"] = data["size"].map(lambda x: size_alias_map.get(x))
        data = data[data["path"].map(lambda x: ".dxf" in x)]
        data["last_modified"] = pd.to_datetime(data["last_modified"])
        data = data.dropna().sort_values(["size_code", "last_modified"])
        # now filter
        return list(data.drop_duplicates(subset=["size_code"], keep="last")["size"])
    except Exception as ex:
        res.utils.logger.warn(
            f"Failed to load the size restriction. Check what we have in the path: {path}"
        )
        return None


def update_bodies(body_codes, body_version_map={}, plan=False):
    """
    set to point to a production env

    os.environ['KAFKA_KGATEWAY_URL'] = "https://data.resmagic.io/kgateway/submitevent"
    os.environ['KAFKA_SCHEMA_REGISTRY_URL'] = "localhost:8001"

    and post with one or more body codes

    usage

        update_bodies('OO-2005', None, plan=True) - inspect payload for default body version
        update_bodies('OO-2005', 1, plan=False) - request a specific body version
        update_bodies(['OO-2005', 'KT-2008'],body_version_map= {'KT-2008':2}, plan=False) - request default version for OO-2005 but version 2 in another body

    """
    from res.connectors.airtable import AirtableConnector

    null = None
    if not body_codes:
        return None
    if isinstance(body_codes, str):
        body_codes = [body_codes]

    if not isinstance(body_version_map, dict):
        body_version_map = {b: body_version_map for b in body_codes}

    filters = AirtableConnector.make_key_lookup_predicate(body_codes, "Body Number")
    res.utils.logger.debug(f"Predicate {filters}")
    body_data = res.connectors.load("airtable").get_airtable_table_for_schema_by_name(
        "meta.bodies", filters=filters
    )
    body_data = body_data[["record_id", "version", "key"]].to_dict("records")

    payloads = []
    for record in body_data:
        rid = record["record_id"]
        version = record["version"]
        body_code = record["key"]
        payloads.append(
            {
                "event_type": "CREATE",
                "requestor_id": rid,
                "job_type": "DXA_EXPORT_BODY_BUNDLE",
                "status": null,
                # opportunity to override the version
                "body_version": body_version_map.get(body_code, version),
                "status_detail": null,
                "result_s3_uri": null,
            }
        )

    if plan:
        return payloads

    kafka = res.connectors.load("kafka")
    return kafka["res_platform.job.event"].publish(payloads, use_kgateway=True)


BODY_BASE = "s3://meta-one-assets-prod/bodies/3d_body_files"

AIRTABLE_META_ONE_BASE = "appa7Sw0ML47cA8D1"
AIRTABLE_BODIES_TABLE = "tblXXuR9kBZvbRqoU"

ADD_BERTHA_JOB_MUTATION = """
	mutation AddNew($detail: jsonb) {
	  insert_dxa_flow_node_run(objects: [{
	    type: DXA_EXPORT_BODY_BUNDLE, 
	    preset_key: "body_bundle_v0", 
        # preset_key: "pieces_v0",
	    details: $detail
	  }]) {
	    returning {
	      id
	      created_at
	    }
	  }
}
"""


def add_bertha_job(body_code, version=None):
    s3 = res.connectors.load("s3")
    airtable = res.connectors.load("airtable")
    hasura = res.connectors.load("hasura")

    body_code_upper = body_code.upper().replace("_", "-")
    body_code_lower = body_code.lower().replace("-", "_")

    # get the latest bw file
    files = list(s3.ls(f"{BODY_BASE}/{body_code_lower}"))

    if version is None:
        bw_file = sorted([f for f in files if f.endswith(".bw")], reverse=True)[0]

        # get the version number from the path
        version_str = bw_file.split("/")[-2]
        if version_str.startswith("v"):
            version_str = version_str[1:]
        version = int(version_str)
    else:
        bw_file = f"{BODY_BASE}/{body_code_lower}/v{version}/{body_code_upper}-V{version}-3D_BODY.bw"
        assert s3.exists(bw_file), f"File {bw_file} does not exist"

    # get the body record id from airtable
    bodies_table = airtable[AIRTABLE_META_ONE_BASE][AIRTABLE_BODIES_TABLE]
    filters = filters = f"""AND(SEARCH("{body_code_upper}",{{Body Number}}))"""
    df = bodies_table.to_dataframe(filters=filters)
    assert len(df) == 1, f"Found {len(df)} bodies for {body_code}"
    record_id = df.iloc[0]["_record_id"]

    # construct payload
    detail = {
        "body_id": record_id,
        "body_code": body_code_upper,
        "body_version": version,
        "input_file_uri": bw_file,
    }

    print(detail)

    hasura.execute_with_kwargs(ADD_BERTHA_JOB_MUTATION, detail=detail)


def get_approved_mat_lookup():
    """
    approved materials relate to bodies - we should pick one as a default for generating sample meta ones for now
    """
    airtable = res.connectors.load("airtable")
    tab = airtable["appa7Sw0ML47cA8D1"]["tbl89gdhA3JZQmyYs"].to_dataframe(
        fields=["Name", "Key"]
    )

    lu = dict(tab[["record_id", "Key"]].values)

    def map_list(s):
        if isinstance(s, list):
            # choosing the first one from the list
            return [lu[i] for i in s][0]
        if pd.notnull(s):
            return lu[s]
        return None

    return map_list


def get_size_lookup():
    """
    the size lookup maps aliases we use in processing
    """
    airtable = res.connectors.load("airtable")
    sizes_lu = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
    # predicate on sizes that we have
    _slu = sizes_lu.to_dataframe(fields=["_accountingsku", "Size Normalized"])
    slu = dict(_slu[["_accountingsku", "Size Normalized"]].values)
    slu_inv = dict(_slu[["Size Normalized", "_accountingsku"]].values)

    return slu_inv


def get_size_chart_lookup():
    """
    the size lookup maps aliases we use in processing
    """
    airtable = res.connectors.load("airtable")
    sizes_lu = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
    # predicate on sizes that we have
    _slu = sizes_lu.to_dataframe(fields=["_accountingsku", "Size Chart"])
    slu = dict(_slu[["_accountingsku", "Size Chart"]].values)
    slu_inv = dict(_slu[["Size Chart", "_accountingsku"]].values)

    return slu_inv


def get_material_properties(materials=None):
    """
    materail properties define physical aspects of a material used in validating assets
    """
    from res.connectors.airtable import AirtableConnector

    airtable = res.connectors.load("airtable")

    filters = None
    if materials is not None:
        if not isinstance(materials, list):
            materials = [materials]
        filters = AirtableConnector.make_key_lookup_predicate(
            materials, "Material Code"
        )

    mat_props = airtable.get_airtable_table_for_schema_by_name(
        "make.material_properties", filters=filters
    ).set_index("key")

    try:
        mp = mat_props[mat_props["material_stability"].notnull()].drop(
            ["record_id", "__timestamp__", "order_key"], 1
        )
    except Exception as ex:
        res.utils.logger.warn(f"Unable to match materials {materials}: {repr(ex)}")
        raise ex

    return dict(zip(mp.index, mp.to_dict("records")))


def get_body_details(bodies, versions=None):
    """
    the body is independent of the request and proides some data about defaults
    """
    from res.connectors.airtable import AirtableConnector

    airtable = res.connectors.load("airtable")

    if not isinstance(bodies, list):
        bodies = [bodies]

    map_list_fn = get_approved_mat_lookup()

    filters = AirtableConnector.make_key_lookup_predicate(bodies, "Body Number")
    res.utils.logger.debug(f"Predicate {filters}")
    body_data = airtable.get_airtable_table_for_schema_by_name(
        "meta.bodies", filters=filters
    )

    res.utils.logger.debug(f"Fetched {len(body_data)} bodies")

    body_data["bkey"] = body_data.apply(
        lambda row: f"{row['key']}-V{row['version']}", axis=1
    )
    body_data["default_material"] = body_data["default_material"].map(map_list_fn)

    body_data = body_data.rename(columns={"key": "body_code"})

    filters = AirtableConnector.make_key_lookup_predicate(bodies, "Body Number")
    br = airtable.get_airtable_table_for_schema_by_name(
        "meta.body_requests", filters=filters
    ).rename(columns={"key": "bkey"})

    res.utils.logger.debug(
        f"Fetched {len(body_data)} bodies and {len(br)} requests - if the join fails its because we cannot find a versioned body request that matche meta number"
    )

    bd = pd.merge(body_data, br, how="left", on="bkey", suffixes=["", "_request"])[
        [
            "bkey",
            "body_code",
            "version",
            "available_sizes",
            "default_material",
            # we could use the request id but seems flaky
            "record_id",
        ]
    ]
    # ensure one primary material then add size mapp and material properties

    # there is an exception for mapping
    slu_inv = get_size_chart_lookup()

    def mapper(v):
        return slu_inv.get(v.lstrip().rstrip())

    bd["size_map"] = bd["available_sizes"].map(
        lambda x: {k.lstrip().rstrip(): mapper(k) for k in x.split(",")}
    )

    bd = bd.rename(
        columns={
            "version": "body_version",
            "bkey": "unit_key",
            "record_id_request": "record_id",
        }
    )

    res.utils.logger.debug(f'Default materials {list(bd["default_material"].values)}')

    mps = get_material_properties(list(bd["default_material"].values))

    bd["material_properties"] = bd["default_material"].map(
        lambda x: {
            k.lstrip().rstrip(): mps.get(k.lstrip().rstrip()) for k in x.split(",")
        }
    )

    return bd


def make_body_default_payloads(
    asset, default_size_only=False, flow="2d", generate_color=False
):
    """
    We can convert body data into meta one creation requests
    this could be used to expand an asset from a kafka payload to an internal handler payload

    """

    from res.connectors.s3.datalake import list_astms

    s3 = res.connectors.load("s3")

    body, version = asset["body_code"], asset["body_version"]

    # use the flow to get the correct astms - for 3d we use a different path
    path = list_astms(
        asset["body_code"], min_version=asset["body_version"], flow=flow
    ).iloc[0]["path"]

    # dxf = s3.read(path)

    if default_size_only:
        dxf = s3.read(path)

        sample_size_gerber = dxf.sample_size
        res.utils.logger.info(
            f"restricting sizes to the sample size {sample_size_gerber} in {path}"
        )

        sizes = {
            k: v
            for k, v in asset["size_map"].items()
            if dxf.map_res_size_to_gerber_size(k, ignore_errors=True)
            == sample_size_gerber
        }

        # TODO some mapping needed to get the mapping between what DXF calls sample size and what the normed calls it
        asset["size_map"] = sizes

        res.utils.logger.debug(
            f"Filtered the size map and sizes to the sample size {asset['size_map']}"
        )

    sizes = asset["size_map"].values()

    reversed_size_map = {v: k for k, v in asset["size_map"].items()}

    res.utils.logger.info(
        f"Processing body {body} v{version} with dxf {path} and sizes {list(sizes)}"
    )

    payloads = [
        {
            # this reference id can be used to get the create one cost
            "id": asset["record_id"],
            "unit_key": asset["unit_key"],
            "body_code": asset["body_code"],
            "body_version": int(asset["body_version"]),
            "color_code": "WHITE",  # s["color_code"].upper(),
            "size_code": size,
            # this is added
            "normed_size": reversed_size_map[size],
            "dxf_path": path,
            # optional only if we are generating color
            "pieces_path": None,
            # todo - pre validations can check these defaults exist or blows up
            "piece_material_mapping": {"default": asset["default_material"]},
            "piece_color_mapping": {"default": "WHITE"},
            "material_properties": asset["material_properties"],
            "status": "ENTERED",
            "tags": [],
            # we suffix with body but expect a value like 2d, 3d - this is experimental
            "flow": f"{flow}-body",
            "generate_color": generate_color,
            # record is style
            "unit_type": "RES_BODY",
            "created_at": res.utils.dates.utc_now_iso_string(),
        }
        for size in sizes
    ]

    return payloads


def dxfs_by_size(body, version_number, flow="3d", suffix="extracted"):
    def clean_one_size_fits(s):
        return s
        if s.lower() in ["one size fits all", "one fits all"]:
            return "1"
        return s

    def remove_petite(f):
        f = f.replace("/P-", "/")
        return f

    s3 = res.connectors.load("s3")
    if flow == "3d":
        root = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body.lower().replace('-','_')}/v{version_number}/{suffix}"
        res.utils.logger.info(f"dxf root: {root}")
        fs = [f for f in s3.ls(f"{root}/dxf_by_size") if ".dxf" in f]
        if len(fs):
            # temp - not sure how the petites are used
            size_part_of = lambda f: f.split("/")[-2].replace("P-", "P_")
            # im not sure if anything else understands this value

            return {size_part_of(f): remove_petite(f) for f in fs}

        # BELOW IS DEPRECATED AS WE HAVE ONLY BY-SIZE NOW
        fs = [f for f in s3.ls(f"{root}/dxf/") if ".dxf" in f]

        if len(fs):
            f = fs[0]
            # remove the petite part and map to the standard
            # f = f.replace("/P-", "/")

            dxf = s3.read(f)
            ss = dxf.sample_size
            sizes = dxf.sizes
            res.utils.logger.info(
                f"no sized-by dxf - instead we have sample size {ss} in {f}. Sizes to add are {sizes}"
            )
            return {s: f for s in sizes}

        raise Exception(f"There is no dxf at location {root}")


def get_versioned_body_details(
    body_code,
    body_version,
    flow="3d",
    sample_size_only=False,
    as_body_request=False,
    **kwargs,
):
    """
    THIS IS THE CORE ONE WE WILL USE
    the body is independent of the request and provides some data about defaults
    """
    from res.connectors.airtable import AirtableConnector

    if isinstance(body_version, str):
        body_version = int(float(body_version.lower().replace("v", "")))
    airtable = res.connectors.load("airtable")

    bodies = [b.upper().replace("_", "-") for b in [body_code]]

    map_list_fn = get_approved_mat_lookup()

    filters = AirtableConnector.make_key_lookup_predicate(bodies, "Body Number")
    res.utils.logger.debug(f"Predicate {filters}")
    body_data = airtable.get_airtable_table_for_schema_by_name(
        "meta.bodies", filters=filters
    )

    res.utils.logger.debug(f"Fetched {len(body_data)} bodies")

    body_data["bkey"] = body_data.apply(
        lambda row: f"{row['key']}-V{row['version']}", axis=1
    )
    body_data["default_material"] = body_data["default_material"].map(map_list_fn)

    body_data = body_data.rename(columns={"key": "body_code"})

    filters = AirtableConnector.make_key_lookup_predicate(bodies, "Body Number")
    br = airtable.get_airtable_table_for_schema_by_name(
        "meta.body_requests", filters=filters
    ).rename(columns={"key": "bkey"})

    res.utils.logger.debug(
        f"Fetched {len(body_data)} bodies and {len(br)} requests - if the join fails its because we cannot find a versioned body request that match meta number"
    )

    try:
        br = br.sort_values("__timestamp__").drop_duplicates(
            subset=["bkey"], keep="last"
        )

        bd = pd.merge(body_data, br, how="left", on="bkey", suffixes=["", "_request"])
    except:
        bd = body_data
        res.utils.logger.warn(
            f"Could not get request using passed body version assumption {body_version}"
        )
        bd["version"] = body_version

    bd = bd[
        [
            "bkey",
            "body_code",
            "version",
            "sample_size",
            "available_sizes",
            "default_material",
            # we could use the request id but seems flaky
            "record_id",
        ]
    ]
    # ensure one primary material then add size mapp and material properties

    # there is an exception for mapping
    slu_inv = get_size_chart_lookup()

    def mapper(v):
        return slu_inv.get(v.lstrip().rstrip())

    bd["size_map"] = bd["available_sizes"].map(
        lambda x: {k.lstrip().rstrip(): mapper(k) for k in x.split(",")}
    )

    bd = bd.rename(
        columns={
            "version": "body_version",
            "bkey": "unit_key",
            "record_id_request": "record_id",
        }
    )

    bd["body_version_string"] = bd["body_version"].map(lambda x: f"V{x}")

    res.utils.logger.debug(f'Default materials {list(bd["default_material"].values)}')

    mps = get_material_properties(list(bd["default_material"].values))

    bd["material_properties"] = bd["default_material"].map(
        lambda x: {
            k.lstrip().rstrip(): mps.get(k.lstrip().rstrip()) for k in x.split(",")
        }
    )

    assert (
        len(bd) == 1
    ), f"There are multiple results in the body query which should not happen: len = {len(bd)}"

    d = dict(bd.iloc[0])

    # we want to start preparing a new body version/assets in advance so the request version might be different from the body current version
    # assert (
    #     d["body_version"] == body_version or body_version is None
    # ), f"expecting the body request version to be the same as the body version {d['body_version']} != {body_version}"

    map_dxfs = dxfs_by_size(d["body_code"], d["body_version"])
    sizes = list(map_dxfs.keys())

    def map_size(s):
        return DxfFile.map_res_size_to_gerber_size_from_size_list(sizes, str(s))

    alias_map = {map_size(s): s for s in d["size_map"].keys()}
    # print(alias_map)
    d["gerber_to_res_size_map"] = alias_map

    inverted_alias_map = {v: k for k, v in alias_map.items()}

    d["sample_size_gerber"] = inverted_alias_map.get(d["sample_size"], d["sample_size"])

    d["dxf_map"] = {
        alias_map.get(k): v for k, v in map_dxfs.items() if alias_map.get(k) is not None
    }

    if as_body_request:
        d = meta_one_payload_from_body_info(
            d, sample_only=sample_size_only, flow=flow, **kwargs
        )

        if kwargs.get("send_test_requests", False):
            res.connectors.load("kafka")["res_meta.meta_one.requests"].publish(
                d, use_kgateway=True, coerce=True
            )

    return d


def meta_one_payload_from_body_info(asset, sample_only=True, flow="3d", **kwargs):
    """
    example

    r = get_versioned_body_details({"KT-2011":"V9"})
    meta_one_payload_from_body_info(r, update_sew=True)

    """
    tags = kwargs.get("tags") or []

    considered_sizes = [
        s
        for s in asset["size_map"].keys()
        if not sample_only or s == asset["sample_size"]
    ]

    payloads = [
        {
            # this reference id can be used to get the create one cost
            "id": asset["record_id"],
            "unit_key": asset["unit_key"],
            "body_code": asset["body_code"],
            "body_version": int(asset["body_version"]),
            "color_code": "WHITE",  # s["color_code"].upper(),
            "size_code": asset["size_map"][res_size],
            "sample_size": asset["sample_size_gerber"],
            # this is added
            "normed_size": res_size,
            "dxf_path": asset["dxf_map"][res_size],
            # optional only if we are generating color
            "pieces_path": None,
            # todo - pre validations can check these defaults exist or blows up
            "piece_material_mapping": {"default": asset["default_material"]},
            "piece_color_mapping": {"default": "WHITE"},
            "material_properties": asset["material_properties"],
            "status": "ENTERED",
            "tags": tags,
            # we suffix with body but expect a value like 2d, 3d - this is experimental
            "flow": f"{flow}-body",
            # for the body we always do this
            "generate_color": True,
            # record is style
            "unit_type": "RES_BODY",
            "created_at": res.utils.dates.utc_now_iso_string(),
            # "metadata": options,
        }
        for res_size in considered_sizes
    ]

    return payloads


def get_body_request_payloads(
    bodies,
    flow="3d",
    default_size_only=False,
    make_meta_one=False,
    default_color=None,
):
    """
    top level function  to get requests for bodies and optionally create the meta-one

    We can either generate the payload and e.g. publish it to kafka for processing or
     we can just directly test processing

    ['TK-2037', 'TK-3101']

    use would be to have a REST endpoint listening to data lake updates on body assets
    then we can trigger the job via kafka messages. The handler should be able to  deal with the different flows

    decoupling of the sew interface would be useful but hard to know when to trigger just now
    so, we should pull the body data from airtable for the onboard request - maybe in the meta one for now
    when the meta one is created we should push the published pieces

    the way this works is the payload generator can expand the handlers full data
    but we have different ways to resolve the attributes on different flows which needs some work
    maybe we group by flows and then collect and expand

    """
    # this is how we generate requests for bodies latest version on a given flow
    if not isinstance(bodies, list):
        bodies = [bodies]

    bodies = get_body_details(bodies)

    for asset in bodies.to_dict("records"):
        payloads = make_body_default_payloads(
            asset, flow=flow, default_size_only=default_size_only
        )
        for payload in payloads:
            if not make_meta_one:
                yield payload
            else:
                # decorete and create asset
                yield export_meta_one(payload)


"""
end of meta one support
"""


def _clean_path(p):
    return p.lower().replace("-", "_")


@res.flows.flow_node_attributes(name="extract_pieces_for_make", memory="50Gi")
def extract_pieces_for_make(event, context):
    """
    Extract asset payload(s) and extract pieces. TODO: work through contract
    For testing we have very homogenous ONEs. In  general we should lookup styles to know what specs need to be prepared
    """

    result = _extract_pieces_for_make()
    # kafka publish make one spec ready/updated


def _extract_pieces_for_make(
    body="CC-6001",
    version="v8",
    size="2zzsm",
    color="PERPIT",
    materials=["CTW70"],
    **kwargs,
):
    """
    This is the workflow the ensures we can nest pieces
    - check we have all the files we should have key'd by BMC-Size etc.
    - ensure we can match pieces with shapefile metadata by naming ALL the pieces that are saved to S3 by the conventional path
    - export the meta body and the make-ONE spec - the existence of these specs makes the body makeable
    """
    s3 = res.connectors.load("s3")
    body_color_root = _clean_path(
        f"s3://meta-one-assets-prod/color_on_shape/{body}/{version}"
    )
    meta_folder = f"{body_color_root}/.meta"

    def logging_function(path, data):
        s3.write(f"{meta_folder}/{path}", data)

    # for testing these are hardcoded but would be generated as a function of the request
    dxf_path = f"s3://meta-one-assets-prod/bodies/{body}/pattern_files/body_{body}_{version}_pattern.dxf"
    dxf = DxfFile(_clean_path(dxf_path))
    im = s3.read(
        _clean_path(
            f"s3://meta-one-assets-prod/color_on_shape/{body}/{version}/{color}/{size}/rasterized_file.png"
        )
    )

    # this should be resolved
    dxf_size = "SM"
    named_pieces = dxf.get_named_piece_image_outlines(
        size=size, piece_types=DXF_MARKER_PIECE_TYPES
    )

    for name, piece in outlines.enumerate_pieces(
        im, named_piece_outlines=named_pieces, log_file_fn=logging_function
    ):
        name = f"{meta_folder}/pieces/{name}.png"
        s3.write(name, piece)

    # this would be done once on dxf upload but for testing we can upsert
    logger.debug(f"exporting meta body for size {dxf_size}")
    dxf.export_body(sizes=[dxf_size])

    # optionally try nest so we can gate on material properties etc.
    # if kwargs.get("requires_nest_gate"):
    #     from res.learn.optimization.nest import nest_dataframe
    #     nested_pcs = nest_dataframe(dataframes.from_dict(named_pieces,'geometry'))
    #     logging_function("nest.feather", nested_pcs)

    logger.debug(f"exporting ONE-spec for the color {color} and materials {materials}")
    # be sure to assert we inserted the correct number of pieces in the body spec
    return dxf.export_body_specs(sizes=[dxf_size], colors=[color], materials=materials)


def add_attributes(assets, **kwargs):
    return assets


def add_piece_outlines(assets, **kwargs):
    """
    Load all the polygons into a dataframe with semantic piece information
    Later when we produce the nesting we can retain that information

    This lives in res.meta.bodies

    assets confirm to a schema, they have a body_code, size, version etc. that define a unique style
    """
    s3 = res.connectors.load("s3")

    def pattern_file_path_from_asset(asset):
        body_code = asset["body_code"].lower()
        # TODO: we need to be very careful here - this sees to be the format but we can be passing two codes around
        body_code = f"{body_code[:2]}_{body_code[2:]}"
        # the body version code is the vN version of the body code
        version = asset["body_version_code"].lower()
        return f"s3://{BODIES_S3_ROOT}/{body_code}/pattern_files/body_{body_code}_{version}_pattern.dxf"

    data = []
    for a in assets.to_dict("records"):
        path = pattern_file_path_from_asset(a)
        if not s3.exists(path):
            raise FileNotFoundError(
                f"The S3 file {path} does nto exist - check that the body exists and the version is correct"
            )
        dxf_data = s3.read(path).piece_boundaries
        dxf_data = dxf_data[dxf_data["size"] == a["size_code"]].reset_index(drop=True)

        outlines = outlines_from_point_set_groups(dxf_data)
        outlines["body_code"] = a["body_code"]
        data.append(outlines)

    all_outlines = pd.concat(data).reset_index(drop=True)
    all_outlines == res.utils.dataframes.rename_and_whitelist(
        all_outlines,
        columns={"body_code": "body_code", "outline_dpi_300": "outline_dpi_300"},
    )
    return pd.merge(assets, all_outlines, on="body_code", how="left")


def images_generator_for_assets(assets, **kwargs):
    yield None


def get_body_sample_sizes_acc_format(keys):
    from res.flows.dxa.styles.helpers import get_bodies

    def _get_style_sample_sizes():
        def c(b):
            return "-".join(b.split(" ")[0].split("-")[:2])

        mapping = {s: c(s) for s in set(keys)}
        s_bodies = list(set(mapping.values()))
        res.utils.logger.info(f"fetch bodies {s_bodies}")
        s_bodies = dict(get_bodies(s_bodies)[["key", "sample_size"]].values)
        mapping = {s: s_bodies.get(b) for s, b in mapping.items()}
        return mapping

    bss = _get_style_sample_sizes()
    sizes_lu = res.connectors.load("airtable")["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
    _slu = sizes_lu.to_dataframe(
        fields=["_accountingsku", "Size Normalized", "Size Chart"]
    )
    slu = dict(_slu[["Size Chart", "_accountingsku"]].values)
    return {k: slu[v] for k, v in bss.items()}


def get_sample_size_body_requests_for_bodies(body_codes, sample_size_only=True):
    """
    when we request meta ones we can get the old payloads to retry
    the sample size only is useful for the body because actually we only really need that for validation
    """
    if isinstance(body_codes, str):
        body_codes = [body_codes]
    body_codes_string = ",".join(f"'{b}'" for b in body_codes)

    Q = f""" with meta_one_data as ( 
     SELECT distinct 
             RECORD_CONTENT as "asset",
              parse_json(RECORD_CONTENT):body_code::string as "body_code" ,
                 parse_json(RECORD_CONTENT):id::string as "id" ,
                 parse_json(RECORD_CONTENT):size_code::string as "size_code" ,
                 parse_json(RECORD_CONTENT):unit_key::string as "unit_key",
                 parse_json(RECORD_CONTENT):flow::string as "flow",
                    TO_TIMESTAMP_TZ(parse_json(RECORD_CONTENT):created_at) as "created_at"
         FROM IAMCURIOUS_PRODUCTION."KAFKA_RES_META_META_ONE_REQUESTS"
          WHERE "flow" = '3d-body' and "body_code" in ({body_codes_string})
         ) ,
          latest as ( 
           SELECT "id", "size_code", max("created_at") as "created_at"
           from meta_one_data group by "id", "size_code"
          )
         select a.* from meta_one_data    
         a join latest b on a."id" = b."id"  and a."size_code" = b."size_code"   and a."created_at" = b."created_at";
     """

    req = res.connectors.load("snowflake").execute(Q)
    req["asset"] = req["asset"].map(json.loads)

    if sample_size_only:
        bss = get_body_sample_sizes_acc_format(req["body_code"].unique())
        req["sample_size"] = req["body_code"].map(lambda x: bss.get(x))
        req = req[req["size_code"] == req["sample_size"]].reset_index()

    return list(req["asset"])


def get_bodies_metadata(keys):
    from res.connectors.airtable import AirtableConnector

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


GET_BERTHA_LAST_RUN_QUERY = """
    query get_latest_errors($details: jsonb!) {
        dxa_flow_node_run(
            where: {
                details: {_contains: $details}, 
                preset_key: {_eq: "body_bundle_v0"}
            }, 
            order_by: {updated_at: desc}, 
            limit: 1
        ) {
            status
            status_details
            created_at
            updated_at
            started_at
            ended_at
        }
    }
"""


def _get_body_metadata(body_code):
    Q = """query body($code: String){
                body(number:$code){
                name
                code
                onboardingMaterials{
                    code
                }
                availableSizes{
                    code
                }
                basePatternSize{
                    code
                }
                patternVersionNumber
                bodyOneReadyRequests(
                    first: 10
                    sort: [{ field: CREATED_AT, direction: DESCENDING }]
                    ) {
                    bodyOneReadyRequests {
                        id
                        bodyVersion
                        status
                    }
                }
             }
            }"""
    g = res.connectors.load("graphql")
    r = g.query_with_kwargs(Q, code=body_code)
    d = r["data"]["body"]
    d["body_code"] = d["code"]
    d["body_version"] = d["patternVersionNumber"]
    requests = d.get("bodyOneReadyRequests", {}).get("bodyOneReadyRequests")
    d["request_id"] = requests[0]["id"] if len(requests) else None
    d["status"] = requests[0]["status"] if len(requests) else None
    d["id"] = res.utils.res_hash()
    d["sizes"] = [f["code"] for f in d["availableSizes"]]
    d["sample_size"] = d["basePatternSize"]["code"]
    return d


def get_body_asset_status(body_code, body_version=None, full_checks=True):
    s3 = res.connectors.load("s3")
    hasura = res.connectors.load("hasura")

    d = _get_body_metadata(body_code)
    for col in ["basePatternSize", "availableSizes"]:
        if col in d:
            d.pop(col)

    # d = {
    #     "body_code": body_code,
    #     "body_version": body_version,
    # }
    # if full_checks or body_version is None:
    #     try:
    #         bc = dict(get_bodies_metadata(body_code).iloc[0])
    #         d["sample_size"] = bc["sample_size"]
    #         d["current_version"] = bc["version"]
    #         d["available_sizes"] = bc["available_sizes"]
    if body_version is None:
        d["body_version"] = body_version = int(d["body_version"])
    d["max_body_version"] = int(d["body_version"])
    body_lower = body_code.replace("-", "_").lower()
    body_version_str = int(float(str(body_version).replace("v", "")))
    body_version_str = f"v{body_version_str}"

    failed_contracts = []
    contract_failure_context = {}
    expected_bw_files = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_lower}/{body_version_str}"

    expected_bw_files = [
        f
        for f in s3.ls(expected_bw_files)
        if ".bw" in f and "3D_BODY" in f and f"V{body_version}" in f
    ]

    if not expected_bw_files:
        failed_contracts.append("3D_BODY_FILE")
    else:
        filename = d["bw_path"] = expected_bw_files[0]
        # check the file name at least the V
        chk = filename.split("/")[-1].split(".")[0]
        if chk.split("-")[2] != f"V{body_version}":
            failed_contracts.append("3D_BODY_FILENAME")

    expected_extracted_files = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_lower}/{body_version_str}/extracted"
    expected_extracted_files = [
        f for f in s3.ls(expected_extracted_files) if "pieces.json" in f
    ]
    if not expected_extracted_files:
        failed_contracts.append("3D_BODY_FILE_UNPACKED")
    else:
        d["extracted_path"] = expected_extracted_files

    metadata = {}
    try:
        from res.flows.dxa.styles.queries import get_body_header

        from res.flows.dxa.styles.helpers import (
            _body_id_from_body_code_and_version as valid_bid,
        )

        md = get_body_header(body_code, body_version)["meta_bodies"]

        if md:
            # in case there is something invalid saved we can filter it
            md = [m for m in md if m["id"] == valid_bid(body_code, body_version)]
            metadata = md[0].get("metadata") or {}
            if metadata.get("flow") != "3d":
                failed_contracts.append("BODY_PIECES_SAVED")
        # TODO lets see if we need to start checking all the saved pieces
    except Exception as ex:
        res.utils.logger.warn(f"failed to query body header {ex}")
    if len(failed_contracts) == 0:
        res.utils.logger.info(f"Checking the body pieces are extracted")

    details = {"body_code": body_code}
    if body_version is not None:
        details["body_version"] = body_version
    else:
        body_version = details["body_version"]
    last_run = hasura.execute_with_kwargs(GET_BERTHA_LAST_RUN_QUERY, details=details)
    results = last_run.get("dxa_flow_node_run", []) if last_run else []
    if results:
        result = results[0]

        def more_than_hours_ago(field, hours):
            d = result.get(field)
            now = datetime.now()
            delta = timedelta(hours=hours)

            return not d or datetime.fromisoformat(d).replace(tzinfo=None) < (
                now - delta
            ).replace(tzinfo=None)

        status = result.get("status")
        if status == "COMPLETED_ERRORS":
            try:
                error_details = result.get("status_details", {})
                issues = parse_status_details_for_issues(error_details)
                job_contracts = [issue["issue_type_code"] for issue in issues]
                failed_contracts.extend(job_contracts)
                contract_failure_context = {
                    issue["issue_type_code"]: issue["context"] for issue in issues
                }
            except:
                import traceback

                logger.warn(f"Couldn't parse status details {traceback.format_exc()}")
            # check the time, if it's not recent something might have crashed
        elif status == "IN_PROGRESS" and more_than_hours_ago("started_at", 2):
            failed_contracts.append("BERTHA_RAN")
            contract_failure_context["BERTHA_RAN"] = "job stuck in progress"
            # check the time, if it's not recent something might have crashed
        elif status == "NEW" and more_than_hours_ago("created_at", 24):
            failed_contracts.append("BERTHA_RAN")
            contract_failure_context["BERTHA_RAN"] = "job never started"
    else:
        # no result means there's no record of a job requested
        failed_contracts.append("BERTHA_JOB_QUEUED")

    d["contracts_failed"] = failed_contracts
    d["contract_failure_context"] = contract_failure_context
    if metadata.get("copied_from_path"):
        d["copied_from_path"] = metadata.get("copied_from_path")

    return d


def _scrape_bw_files():
    s3 = res.connectors.load("s3")

    all_bw = [
        f for f in s3.ls("s3://meta-one-assets-prod/bodies/3d_body_files") if ".bw" in f
    ]
    df = pd.DataFrame(all_bw, columns=["files"])
    all_bw[0]
    df["filename"] = df["files"].map(lambda x: x.split("/")[-1])
    df["path_version"] = df["files"].map(
        lambda x: x.split("/")[-2].upper().replace("V", "")
    )
    df["fileversion"] = df["filename"].map(
        lambda x: x.split("-")[2].upper().replace("V", "")
    )
    df["valid"] = False
    df["body_code"] = df["filename"].map(lambda x: x[:7])
    df.loc[
        (df["path_version"] == df["fileversion"])
        & (df["filename"].map(lambda x: "3D_BODY" in x)),
        "valid",
    ] = True
    df_filterd = df[df["valid"]]
    return df_filterd
