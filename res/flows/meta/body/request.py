"""
This is a simpler method for unzipping dxf by size and requesting the meta one
"""
import res
from res.flows import FlowContext
from res.flows.meta.bodies import get_versioned_body_details


def get_body_meta_extraction_path(body, version, path, flow, suffix="extracted"):
    """
    body files go to extracted
    """
    if "3d" in flow:
        filename = path.split("/")[-1].split(".")[0]
        root = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body}/{version}/{suffix}"  # /{filename}
        res.utils.logger.info(f"Extracting to {root}")
        return root

    return ""


def handler(event, context={}):
    with FlowContext(event, context) as fc:
        data = fc.args
        body_code = data["body_code"]
        body_code = body_code.upper().replace("_", "-")
        body_code_lower = body_code.lower().replace("-", "_")
        flow = data["flow"]
        default_size_only = data.get("default_size_only", False)
        extract_zip = data.get("extract_zip", True)

        path = data.get("path")

        body_code = data["body_code"]
        body_code = body_code.upper().replace("_", "-")

        body_version = data.get("body_version")
        if body_version:
            body_version = body_version.lower().replace("v", "")
            body_version = f"v{body_version}"

        try:
            if path[-4:] == ".zip":
                # an example extract content is
                #  s3://meta-one-assets-prod/bodies/3d_body_files/dj_6000/v1/extracted/dxf_by_size/P-2X/DJ-6000-V1-3D_BODY.dxf
                # the meta one will look for things under the body body `/extracted` but the "dxf_by_size" comes from the client pack structure
                xpath = get_body_meta_extraction_path(
                    body_code_lower, body_version, path, flow
                )
                if extract_zip:
                    res.utils.logger.debug(f"unpacking {path} to {xpath}")
                    fc.connectors["s3"].unzip(path, xpath, verbose=True)

                # move the poms somewhere

            res.utils.logger.info(f"Posting meta one request for {data}")
            get_versioned_body_details(
                body_code,
                body_version,
                as_body_request=True,
                sample_size_only=default_size_only,
                send_test_requests=True,
            )

            # queue notify
        except Exception as ex:
            # flows.queues.queue_status **
            res.utils.logger.warn(
                f"Failed to process {data} - {res.utils.ex_repr(ex)}", exception=ex
            )

        return {}


def get_sample_payload():
    from res.flows import FlowEventProcessor

    fp = FlowEventProcessor()

    e = fp.make_sample_flow_payload_for_function("meta.body.request")

    e["args"] = {
        "body_code": None,
        "body_version": None,
        # s3://meta-one-assets-prod/bodies/3d_body_files/tk_6121/v3/uploaded/TK-6121-V3-3D_BODY.zip
        "path": None,
        "flow": "3d",
        "default_size_only": True,
        "extract_zip": True,
    }

    return e


"""
example request that is sent to the downstream handler

{'apiVersion': 'resmagic.io/v1',
 'args': {},
 'assets': [{'id': 'rect5n5PPAX4U5aPb',
   'unit_key': 'JR-3114-V2',
   'body_code': 'JR-3114',
   'body_version': 2,
   'color_code': 'WHITE',
   'size_code': '3ZZMD',
   'sample_size': 'M',
   'dxf_path': 's3://meta-one-assets-prod/bodies/3d_body_files/jr_3114/v2/extracted/dxf_by_size/M/JR-3114-V2-3D_BODY.dxf',
   'pieces_path': 'None',
   'piece_material_mapping': {'default': 'LTCSL'},
   'piece_color_mapping': {'default': 'WHITE'},
   'status': 'ENTERED',
   'tags': [],
   'flow': '3d-body',
   'unit_type': 'RES_BODY',
   'created_at': '2022-07-26T01:57:33.870484+00:00',
   'generate_color': True,
   'normed_size': 'M',
   'material_properties': {'LTCSL': {'compensation_length': 1.015228426395939,
     'compensation_width': 1.0582010582010584,
     'paper_marker_compensation_length': 1.0,
     'paper_marker_compensation_width': 1.0,
     'fabric_type': 'Woven',
     'material_stability': 'Stable',
     'pretreatment_type': 'Cotton',
     'cuttable_width': 58.0,
     'offset_size': None,
     'order_key': None}}}],
 'kind': 'resFlow',
 'metadata': {'name': 'meta.marker',
  'version': 'primary',
  'git_hash': 'd1f4626',
  'argo.workflow.name': 'marker-res494d91a812-resa70f9a4dfd'},
 'task': {'key': 'marker-res494d91a812-resa70f9a4dfd'}}

"""
