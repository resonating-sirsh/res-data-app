"""
Sync Media from create.ONE to Shopify.

This process upload media from s3 to Shopify and create the media in Shopify
"""


import datetime
import typing

import pydantic
from bson.objectid import ObjectId

from res.connectors.mongo.MongoConnector import MongoConnector
from res.connectors.s3.S3Connector import S3Connector
from res.connectors.shopify.resource.media import (
    ProductCreateMediaInput,
    ShopifyProductMedia,
)
from res.connectors.shopify.resource.staged_url import (
    HttpMethod,
    StageUploadInput,
    create_staged_upload,
    get_resource_from_mime_type,
    upload_media_to_stage_upload,
)
from res.flows import FlowContext, flow_node_attributes
import enum


class SyncMediaStatus(enum.Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"


class SyncMediaPayload(pydantic.BaseModel):
    alt: str
    s3_paths: typing.List[str]
    product_id: str


class SyncMediaEvent(pydantic.BaseModel):
    store_code: str
    ecommerce_id: str
    payload: typing.List[SyncMediaPayload]


@flow_node_attributes(
    "sync_media_with_shopify.handler",
    description="Sync media from s3 to Shopify",
    memory="2Gi",
    mapped=False,
)
def handler(event, context):
    """
    Sync media from s3 to Shopify

    Create a stage upload in Shopify for each media in s3_paths and then create the media in Shopify
    """
    with FlowContext(event, context) as fc:
        try:
            data = get_args(fc)
            mongo_con: MongoConnector = fc.connectors["mongo"]
            db = mongo_con.get_client().get_database("resmagic")
            media_create_payload = []
            client = ShopifyProductMedia(brand_code=data.store_code)
            for item in data.payload:
                product = db.products.find_one({"_id": ObjectId(item.product_id)})
                if not product:
                    raise Exception("Product not found")

                if not product.get("ecommerceId", None):
                    raise Exception("Sync can't be performance product is not imported")

                # initialize the shopify sdk

                fc.logger.info(f"Syncing media: {event}")
                s3: S3Connector = fc.connectors["s3"]

                def handle_create_staged_upload_payload(s3_path: str):
                    s3_file = s3.get_file_info(s3_path)
                    filename = s3_path.split("/")[-1]
                    mime_type = s3_file["ContentType"]
                    fc.logger.info(f"Creating staged upload for {filename}")
                    fc.logger.info(f"mime_type: {mime_type}")
                    if mime_type == "binary/octet-stream" and "3d.glb" in filename:
                        mime_type = "model/gltf-binary"
                    resource = get_resource_from_mime_type(mime_type)
                    return StageUploadInput(
                        filename=filename,
                        resource=resource,
                        mimeType=mime_type,
                        httpMethod="POST",
                        fileSize=s3_file["ContentLength"],
                    )

                staged_upload_payload = map(
                    handle_create_staged_upload_payload,
                    item.s3_paths,
                )

                staged_targets = create_staged_upload(list(staged_upload_payload))
                # reverse the list to match the order of the s3_paths when calling the list#pop
                staged_targets.reverse()

                for s3_path in item.s3_paths:
                    value = staged_targets.pop()
                    s3: S3Connector = fc.connectors["s3"]
                    s3_file = s3.get_file_info(s3_path)
                    mime_type = s3_file["ContentType"]
                    s3_file_object = s3.file_object(s3_path)
                    raw_file = s3_file_object.read()
                    filename = s3_path.split("/")[-1]
                    upload_media_to_stage_upload(
                        http_method=HttpMethod.POST,
                        media_raw=raw_file,
                        staged_url=value.url,
                        filename=filename,
                        mime_type=mime_type,
                        parameters=value.parameters,
                    )

                    if not value.resourceUrl:
                        raise Exception("Resource url is not present: invalid mimeType")

                    media_create_payload.append(
                        ProductCreateMediaInput(
                            alt=item.alt,
                            mediaContentType=get_resource_from_mime_type(mime_type),
                            originalSource=value.resourceUrl,
                        )
                    )

            fc.logger.info(
                f"Creating media in Shopify (n={len(media_create_payload)}) ..."
            )

            # wipe all the previous media
            client.delete_all_product_media(ecommerce_id=data.ecommerce_id)
            client.create_product_media(
                ecommerce_id=data.ecommerce_id,
                input=media_create_payload,
            )

            fc.metric_incr("media_sync_success")

        except pydantic.errors.PydanticValueError as e:
            fc.logger.error(
                "Fail while parsing the arguments, not valid event",
                exception=e,
                errors=e,
            )
            fc.metric_incr("media_sync_invalid_event")
            raise e
        except Exception as e:
            fc.logger.error(
                "Fail while syncing media",
                exception=e,
                errors=e,
            )
            fc.metric_incr("media_sync_fail")
            raise e
        finally:
            return {}


def on_failure(event, context):
    """
    On case of failure we mark the synced status on the product as failed
    """
    with FlowContext(event, context) as fc:
        fc.metric_incr("media_sync_fail")
        mongo: MongoConnector = fc.connectors["mongo"]
        data = get_args(fc)
        db = mongo.get_client().get_database("resmagic")
        for item in data.payload:
            db.products.update_one(
                {"_id": ObjectId(item.product_id)},
                {
                    "$set": {
                        "mediaSyncedStatus": SyncMediaStatus.ERROR.value,
                    }
                },
            )
    return {}


def on_success(event, context):
    """
    On case of success we mark the synced status on the product as done
    and modify lastMediaSyncedAt as well
    """
    with FlowContext(event, context) as fc:
        fc.logger.info(f"Flow success | event: {event} | context: {context}")
        data = get_args(fc)
        mongo: MongoConnector = fc.connectors["mongo"]
        db = mongo.get_client().get_database("resmagic")
        for item in data.payload:
            db.products.update_one(
                {"_id": ObjectId(item.product_id)},
                {
                    "$set": {
                        "mediaSyncedStatus": SyncMediaStatus.SUCCESS.value,
                        "lastMediaSyncedAt": datetime.datetime.utcnow(),
                    }
                },
            )
    return {}


def get_args(fc: FlowContext) -> SyncMediaEvent:
    payload = fc.args["payload"]
    del fc.args["payload"]
    return SyncMediaEvent(
        **fc.args,
        payload=[SyncMediaPayload(**item) for item in payload],
    )
