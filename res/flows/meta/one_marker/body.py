from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.flows.dxa.styles.helpers import _body_id_from_body_code_and_version
from res.utils import logger
import res

s3 = res.connectors.load("s3")
hasura = res.connectors.load("hasura")

GET_BODY = """
    query body($id: ID!, $bodyVersion: Int!){
        body(id:$id){
            id
            code
            number
            patternVersionNumber
            body3dSimulationFile(bodyVersion: $bodyVersion) {
                file {
                    uri
                }
                bodyVersion
            }
        }
    }
"""

UPDATE_BODY_MUTATION = """
    mutation updateBody($id: ID!, $input: UpdateBodyInput!){
        updateBody(id:$id, input:$input){
            body {
                id
            }
        }
    }
"""

ADD_BODY_3D_MODEL_URIS = """
    mutation update_body_metadata(
        $metadata: jsonb = "", 
        $id: uuid = "", 
        $model_3d_uri: String = "", 
        $point_cloud_uri: String = "",
        $front_image_uri: String = ""
    ) {
        update_meta_bodies(
            where: {id: {_eq: $id}},
            _append: {metadata: $metadata},
            _set: {model_3d_uri: $model_3d_uri, point_cloud_uri: $point_cloud_uri, front_image_uri: $front_image_uri}
        )  {
            returning {
                id
                metadata
            }
        }
    }
"""


def get_dxa_export_body_bundle_details(
    record_id, body_version, input_file_uri, rename_input_file
):
    gql = ResGraphQLClient()

    body = gql.query(
        GET_BODY, variables={"id": record_id, "bodyVersion": body_version}
    )["data"]["body"]

    job_details = {
        "body_id": body["id"],
        "body_code": body["code"],
        "body_version": body_version,
        "input_file_uri": input_file_uri,
        "rename_input_file": rename_input_file,
    }

    return job_details


def process_3d_model(body_code, body_version, path):
    root = "s3://meta-one-assets-prod/bodies/3d_body_files"
    relative = f"{body_code}/v{body_version}".lower().replace("-", "_")
    dest = f"{root}/{relative}/extracted"

    logger.info(f"Unzipping {path} to {dest}")
    s3.unzip(path, dest)

    id = _body_id_from_body_code_and_version(body_code, body_version)
    logger.info(f"Updating '{id}' generated from '{body_code}' and '{body_version}'")
    hasura.execute_with_kwargs(
        ADD_BODY_3D_MODEL_URIS,
        id=id,
        metadata={
            "has_3d_model": True,
            "3d_model": f"{dest}/3d.glb",
            "point_cloud": f"{dest}/point_cloud.json",
            "front_image": f"{dest}/front.png",
        },
        model_3d_uri=f"{dest}/3d.glb",
        point_cloud_uri=f"{dest}/point_cloud.json",
        front_image_uri=f"{dest}/front.png",
    )
