from res.flows.FlowContext import FlowContext
from . import *
import res
from res.flows.meta.marker import MetaMarker
from PIL import Image
import requests
from io import BytesIO
from functools import partial
import json
import numpy as np
from res.observability.entity import AbstractEntity
from res.observability.io import ColumnarDataStore


def save_sew_records_to_store(sew_ops, purge_existing=False):
    Model = AbstractEntity.create_model_from_data(
        "garment_construction_methods_glossary", sew_ops, namespace="sew"
    )
    records = [Model(**record) for record in sew_ops.to_dict("records")]

    if purge_existing:
        s3 = res.connectors.load("s3")
        s3._delete_object(
            "s3://res-data-platform/stores/columnar-store/sew/garment_construction_methods_glossary/parts/0/data.parquet"
        )
    store = ColumnarDataStore(
        Model,
        create_if_not_found=True,
        description="Sewing methods are described in a glossary as they are used to construct garments. Describes stitches, machine usage etc. Examples such as fusing, french seams, stop stiches, thread specifications etc. Use this store to find descriptions, images and examples of sewing methods",
    )
    store.update_index()
    store.add(records, key_field="code")
    return store


def get_image_url(x):

    try:

        return x[0].get("url") if isinstance(x, list) else x.get("url")
    except:
        # raise
        return None


def try_dump_json(x):
    try:
        return json.dumps(x)
    except:
        return str(x)


def save_to_s3(
    row,
    field_name="operation_image",
    table="sew-operations",
    table_id="tblittFEvMLwEXgAD",
    uri_only=True,
):
    uri = f"s3://res-data-platform/airtable/attachments/{table}/{table_id}/{field_name}/{row['record_id']}/image.jpeg"
    if uri_only:
        return uri

    s3 = res.connectors.load("s3")

    try:
        image_uri = row[f"{field_name}_https"]
        if image_uri:
            response = requests.get(image_uri)
            # print(r.headers['Content-Type'].split())
            # print(response)
            img = Image.open(BytesIO(response.content)).convert("RGB")
            s3.write(uri, img)
            return uri
        return None
    except:
        # raise
        return None


# add queue monitors for status on bodies
# meta.construction.sew.sync_sew_interface


def build_graph(purge=True):
    neo = res.connectors.load("neo4j")
    if purge:
        neo.delete_all()
    get_sew_ops_as_sew_model(update_graph_db=True)

    add_seam_ops_to_graph_database(seed_op_sites=True, load_sew_ops=True)


def get_sew_ops_as_sew_model(save_image_uris=False, update_graph_db=False):
    from res.flows.meta.construction.model import SewOperation

    ops = get_sew_ops(save_image_uris=save_image_uris, use_model=SewOperation)

    if update_graph_db:
        res.utils.logger.info(f"updating graph db")
        neo = res.connectors.load("neo4j")
        neo.update_records(ops)
    return ops


def get_sew_op_ids():
    fields = {
        "Name": "code",
        "Sew Friendly Names": "name",
    }
    res.utils.logger.info(f"Loading the sew op code:id mapping")
    airtable = res.connectors.load("airtable")
    sew_ops = (
        airtable["appa7Sw0ML47cA8D1"]["tblittFEvMLwEXgAD"]
        .to_dataframe(fields=list(fields.keys()))
        .rename(columns=fields)
    )

    return dict(sew_ops[["record_id", "code"]].values)


def get_sew_ops(save_image_uris=False, infer_model=None, use_model=None):

    airtable = res.connectors.load("airtable")
    s3 = res.connectors.load("s3")

    fields = {
        "Name": "code",
        "Sew Friendly Names": "name",
        "Operation Time Attributes": "operation_time_attributes",
        "Sew Attribute": "sew_attribute",
        "Operation Type - Body": "body_operation_type",
        "Operation Image": "operation_image",
        "Machine": "machines",
        "Operation Intention Attributes": "operation_intention",
        "Brand Facing Category": "industry_name",
        "Machine Accessories": "machine_accessories",
        "Operation Specification": "operation_specification",
        "Sew Symbol Types (from 3D Construction)": "sew_symbol_types",
        "3D Margin - Distance from Edge": "margin_distance_to_edge",
        "Seam Allowance (from 3D Construction)": "seam_allowance",
        "Default SA Value ≥": "default_sean_allowance",
        "Sew Friendly Names (Spanish)": "spanish_name",
        "Machinery Photo": "machine_photo",
        "Visibel Stitch Attributes": "visible_stitch_attributes",
        "Operation Status in Practice": "operation_best_practice_category",
        "Operation Time Attributes": "operation_time_attributes",
        "Paper Stamper Intention": "stamper_information",
        #'Body (File) Impact' : 'body_file_impact'
    }

    def list_mapper(lu):
        def f(x):
            if isinstance(x, list):
                return [lu.get(i) for i in x if pd.notnull(i)]
            elif pd.isnull(x):
                return []
            return x

        return f

    piece_op = airtable.get_table_data("appa7Sw0ML47cA8D1/tbljcgSOfmHaHbv4U")
    c = {
        "_english_parts": "part",
        "Name (from Sew Operation)": "operation_name",
        #'record_id': 'operation_name'
    }

    piece_op = piece_op.rename(columns=c)
    piece_op = piece_op.drop([i for i in piece_op.columns if i not in c.values()], 1)
    piece_op["operation_name"] = piece_op["operation_name"].map(
        lambda x: x[0].strip() if isinstance(x, list) else str(x).strip()
    )
    parts_lu = dict(
        piece_op.groupby("operation_name").agg({"part": set}).reset_index().values
    )

    machines = airtable["appa7Sw0ML47cA8D1"]["tbly7p1V10xW2ED4k"].to_dataframe()
    machine_accessories = airtable["appa7Sw0ML47cA8D1"][
        "tblMmjmXOa0UxZMZ1"
    ].to_dataframe()
    machine_accessories["Name"] = machine_accessories["Name"].map(
        lambda x: x.rstrip().lstrip()
    )
    machines_lu = dict(machines[["record_id", "Name"]].values)
    machines_acc_lu = dict(machine_accessories[["record_id", "Name"]].values)

    sew_ops = airtable["appa7Sw0ML47cA8D1"]["tblittFEvMLwEXgAD"].to_dataframe(
        fields=list(fields.keys())
    )
    sew_ops = sew_ops.rename(columns=fields)

    sew_ops["machines"] = sew_ops["machines"].apply(list_mapper(machines_lu))
    sew_ops["first_machine"] = sew_ops["machines"].apply(
        lambda x: x[0] if len(x) > 0 else None
    )
    sew_ops["second_machine"] = sew_ops["machines"].apply(
        lambda x: x[1] if len(x) > 1 else None
    )

    sew_ops["machine_accessories"] = sew_ops["machine_accessories"].map(
        list_mapper(machines_acc_lu)
    )

    for field in [
        "body_operation_type",
        "machine_photo",
        "sew_attribute",
        "operation_specification",
        "industry_name",
        "visible_stitch_attributes",
        "stamper_information",
    ]:
        sew_ops[field] = sew_ops[field].map(
            lambda x: x[0] if isinstance(x, list) else None
        )
    sew_ops = res.utils.dataframes.replace_nan_with_none(sew_ops)
    sew_ops["operation_image_https"] = sew_ops["operation_image"].map(get_image_url)
    sew_ops["machine_photo_https"] = sew_ops["machine_photo"].map(get_image_url)

    save_op_image = partial(save_to_s3, field_name="operation_image")
    save_machine_image = partial(save_to_s3, field_name="machine_photo")
    sew_ops["operation_image_s3"] = sew_ops.apply(save_op_image, axis=1)
    sew_ops["machine_photo_s3"] = sew_ops.apply(save_machine_image, axis=1)
    sew_ops["parts_applied"] = sew_ops["code"].map(lambda x: parts_lu.get(x))
    sew_ops["parts_applied"] = sew_ops["parts_applied"].map(
        lambda x: [a for a in x if not pd.isnull(a)] if isinstance(x, set) else []
    )
    sew_ops = sew_ops.drop(
        [
            "operation_image",
            "operation_image_https",
            "machine_photo_https",
            "machine_photo",
        ],
        1,
    )

    # ai_desc = pd.read_pickle('/Users/sirsh/Downloads/GPT-Sew_descriptions.pkl')
    # from res.observability.dataops import parse_fenced_code_blocks
    # def f(x):
    #     try:
    #         return parse_fenced_code_blocks(x,select_type='json')[0]
    #     except:
    #         return {'unparsed': x}

    # ai_desc['description'] = ai_desc['description'].map(f)
    # ai_desc = dict(ai_desc.values)
    # sew_ops['ai_description'] = sew_ops['code'].map(lambda x: ai_desc.get(x))
    # sew_ops['ai_description'] = sew_ops['ai_description'].map(f)

    sew_ops["name"] = sew_ops["name"].map(lambda x: x.strip() if pd.notnull(x) else x)
    for o in ["machines", "machine_accessories", "parts_applied"]:
        sew_ops[o] = sew_ops[o].map(
            lambda x: list(x) if isinstance(x, np.ndarray) else x
        )

    sew_ops["name"] = sew_ops["name"].fillna(sew_ops["code"])
    sew_ops = sew_ops[sew_ops["code"].notnull()]
    sew_ops["code"] = sew_ops["code"].map(lambda x: x.strip())
    sew_ops["name"] = sew_ops["name"].map(lambda x: x.strip())

    if infer_model:
        Model = AbstractEntity.create_model_from_data(
            "garment_construction_methods_glossary", sew_ops, namespace="sew"
        )
        return [Model(**record) for record in sew_ops.to_dict("records")]
    if use_model:
        return [use_model(**record) for record in sew_ops.to_dict("records")]

    return sew_ops


def get_seam_ops_as_seam_op_model(update_graph_db=False):
    from res.flows.meta.construction.model import SeamOperationSite

    ops = get_seam_ops()

    ops = [SeamOperationSite(**o) for o in ops]

    if update_graph_db:
        res.utils.logger.info(f"updating graph db")
        neo = res.connectors.load("neo4j")
        neo.update_records(ops)
    return ops


def get_seam_ops():
    """
    https://airtable.com/appa7Sw0ML47cA8D1/tbl0VHFFA0A4RFvoD/viwLsPtRJxlyhQkxM?blocks=hide
    seam construction library maps seam regions ot operations

    we can add to neo4j our config db

    """
    airtable = res.connectors.load("airtable")
    data = airtable.get_table_data("appa7Sw0ML47cA8D1/tbl0VHFFA0A4RFvoD")
    c = {
        "Body Number": "body_code",
        "Operation Status on Operation (from Link to Sew Operation)": "operation_status",
        "Brand Facing Name": "industry_term",
        "_location_english": "operation_site_code",
        "Construction Type": "construction_operation_type",
        "body construction": "seam_operation_code",
        "WHERE": "name",
        "Link to Sew Operation": "sew_op_id",
        "Location Types": "site_type",
        "Res.Default (Piece x Operation)": "code",
        "Sew Friendly Name( from Sew operation Table )": "sew_operation_name",
    }

    data = data.rename(columns=c)
    data = data.drop([i for i in data.columns if i not in c.values()], 1)
    data["seam_operation_code"] = data["seam_operation_code"].map(
        lambda x: x.strip() if pd.notnull(x) else None
    )
    data["sew_op_id"] = data["sew_op_id"].map(
        lambda x: x[0] if isinstance(x, list) else x
    )
    return data


def add_seam_ops_to_graph_database(
    seed_op_sites=True, load_sew_ops=True, relationships_only=False
):
    """
    seam the seam operations - we can link to sites by code
    if seam operations maps are added we can also add those relationships
    """
    sites = get_seam_ops()
    from res.flows.meta.construction.model import (
        SeamOperationSiteCategory,
        SeamOperationSite,
        GarmentSeamOpRel,
        GarmentSewOpRel,
        SewOperationRef,
    )

    n = res.connectors.load("neo4j")

    # use the operation sites as keys
    if seed_op_sites:
        res.utils.logger.info(f"Add locations - these map from construction language")
        records = [
            SeamOperationSiteCategory(**s)
            for s in sites.rename(columns={"operation_site_code": "code"}).to_dict(
                "records"
            )
        ]
        n.add_records(records)

    # now add the seam ops with link to sites
    records = [SeamOperationSite(**s) for s in sites.to_dict("records")]

    res.utils.logger.info(f"Add seam operations and links to sites")

    n.add_records(records)

    # REL Seam-op -> Sew Op Site
    for record in sites.to_dict("records"):
        b = SeamOperationSiteCategory(code=record["operation_site_code"])
        a = SeamOperationSite(**record)
        n.add_relationship(a, b, GarmentSeamOpRel())

    # can be read in from parquet file
    # sew_op_records = [SewOperation(**s) for s in sew_ops.to_dict('records')]
    # sew_op_map = {r.record_id:r for r in sew_op_records}

    ## relation SEAM-OP -> SEW_OP
    if load_sew_ops:
        res.utils.logger.info(f"reloading sew ops to apply the mapping to seam ops")
        sew_op_map = get_sew_op_ids()

        res.utils.logger.info(f"Add seam operation links to sew operations")
        for record in sites.to_dict("records"):
            try:
                b = sew_op_map[record["sew_op_id"]]
                b = SewOperationRef(code=b)
                a = SeamOperationSite(**record)
                n.add_relationship(a, b, GarmentSewOpRel())
            except Exception as ex:
                res.utils.logger.warn(f"Missing {record['seam_operation_code']} - {ex}")

    res.utils.logger.info(f"Done")
    return sites


def add_sew_ops():
    from res.flows.meta.construction.sew import get_sew_ops

    neo = res.connectors.load("neo4j")

    records = get_sew_ops(as_model=True)
    neo.add_records(records)

    return records


def get_part_seam_relation(part_name, locations):
    """
    use GPT to determine if the parts could be related to the seam so we can add relationships

    for example

    ```
    for region in a['seams']:
    if region['probability'] >= 0.5:
        nd = GarmentPart(code='BKYKE', name='Back Yoke', region= 'Back Bodice')
        r = GarmentPartSeamRel(comment=region['comment'])
        md = SeamOperationSiteCategory(code=region['name'])
        n.add_relationship(nd, md, r)

    ```
    """
    from res.learn.agents.BodyOnboarder import BodyOnboarder

    P = f"""
    From the list of seam locations below, specify all the seams the `{part_name}` would be connected to.
    
    **List**
    ```json
    {json.dumps(locations)}
    ```
    
    Respond in a json list with location name and comment for locations that could be connected to the `{part_name}`. 
    Please be exhuastive in your list and add a probability from 0 to 1 that the seam might connect to the piece
    
    Use the response format below
    
    ** Response Model **
    class SeamInfo(BaseModel):
        name: str
        comment: str
        probability: float
        
    class SeamConnections(BaseModel):
      seams: List[SeamInfo]
    
    """

    b = BodyOnboarder()
    a = json.loads(b.ask(P, response_format={"type": "json_object"}))
    return a


########
##
##########################################


def handle_sew_requests(event, context=None):
    """
    This function will look into the sew construction tables and take some actions on an interval
    for example we can refetch BW files and run the entire meta one re-creation or just recreate the meta one - these are hard and soft resets
    """
    handle_sew_bot_requests()


def get_body_timestamps():
    """
    Looks at the sew assignation table and gets the lastest change for any body
    """
    airtable = res.connectors.load("airtable")
    tab = airtable["appa7Sw0ML47cA8D1"]["tblUGsbA6ApGp3ZXc"]
    data = tab.to_dataframe()
    lookups = data[["Body Number", "record_id", "Last Modified"]]
    lookups["body_code"] = lookups["Body Number"].map(
        lambda x: x[0] if isinstance(x, list) else None
    )
    return dict(
        lookups[["body_code", "Last Modified"]]
        .sort_values("Last Modified")
        .drop_duplicates(subset=["body_code"], keep="last")
        .dropna()
        .values
    )


@res.flows.flow_node_attributes(
    memory="52Gi",
)
def sync_sew(event, context=None):
    """
    We make sure that the sew dataframe is maintained for the body and make sure that previews are stored for the bodies
    We use the timestamp on the sew dataframe and the last updated dates on the sew interface

      meta.construction.sew.sync_sew


        from res.flows.meta.construction import get_sew_assignation_for_body, load_table
        sew_df = load_table(body='TH-1001')
        get_sew_assignation_for_body('TH-1001')
        sew_df
        from res.flows.meta.construction.sew import sync_sew
        from warnings import filterwarnings
        filterwarnings('ignore')
        sync_sew({},{})

    """

    # list the bodies in the interface
    # check some watermark or hash
    with FlowContext(event, context) as fc:
        res.utils.logger.info(f"Managing sew interfaces - loading bodies to sync")

        s3 = res.connectors.load("s3")

        bodies = get_body_timestamps()

        publish_meta_one_samples = fc.args.get("publish_meta_one_samples", True)
        modified_since = fc.args.get("modified_since")
        force = fc.args.get("force", False)
        status = []
        for body, ts in bodies.items():
            ts = res.utils.dates.parse(ts, False)
            body_lower = body.replace("-", "_").lower()
            path = f"s3://meta-one-assets-prod/bodies/sew/{body_lower}/edges.feather"
            s = {"key": body, "got_assignation": False, "pushed_previews": False}

            if s3.exists(path) and not force:
                info = list(s3.ls_info(path))[0]
                last_mod = info["last_modified"]
                last_mod = last_mod.replace(tzinfo=None)

                if ts < last_mod:
                    res.utils.logger.info(
                        f"Skipping body {body} not modified since it was last saved on {info['last_modified']}"
                    )
                    continue

            if modified_since:
                modified_since = res.utils.dates.parse(modified_since)
                if ts < modified_since:
                    res.utils.logger.info(
                        f"Skipping body {body} not modified after parameter {modified_since} "
                    )
                    continue

            res.utils.logger.info(f"migrating the sew data for the body {body}")

            try:
                sew_df = get_sew_assignation_for_body(body=body)
                s3.write(
                    path,
                    sew_df,
                )
                s["got_assignation"] = True
            except Exception as ex:
                res.utils.logger.warn(repr(ex))
                res.utils.logger.warn(f"This one failed {body}... continue")

            res.utils.logger.info("Saved the sew data for this body")

            # loading the meta one

            if publish_meta_one_samples:
                try:
                    res.utils.logger.info(
                        "Syncing meta one data using a rep for the body"
                    )
                    # resolve somehow
                    meta_one = MetaMarker.find_meta_one(body=body)
                    if len(meta_one):
                        meta_one = meta_one.iloc[0]["path"]
                        meta_one = MetaMarker(meta_one)
                        # add tenacity to this guy's guts
                        meta_one.publish_pieces_with_res_color_layers(
                            use_kafka_to_update=False,
                        )
                        s["pushed_previews"] = True
                except Exception as ex:
                    res.utils.logger.warn(
                        f"Failed to generate sew preview for body {body}: {repr(ex)}"
                    )

                # published
                res.utils.logger.info("published preview pieces")
            status.append(s)

        res.utils.logger.info(s)
        res.utils.logger.info("Synced sew interface for bodies")

        return status
