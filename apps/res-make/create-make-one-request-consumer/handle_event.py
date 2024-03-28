import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.flows.FlowContext import FlowContext
from res.utils import logger, ping_slack, post_with_token
from schemas.pydantic.make import MakeAssetRequest
from res.flows.sell.orders.process import pull_missing_order_if_not_exists

KAFKA_JOB_EVENT_TOPIC = "res_make.make_one_request.create"

# nudge....


def kafka_message_to_make_request(event):
    body_code = event.get("body_code")
    material_code = event.get("material_code")
    color_code = event.get("color_code")
    size_code = event.get("size_code")
    sku = f"{body_code} {material_code} {color_code} {size_code}"
    return MakeAssetRequest(
        sku=sku,
        body_version=int(event.get("body_version")),
        brand_code=event.get("brand_code"),
        brand_name=event.get("brand_name"),
        style_id=event.get("style_id"),
        style_version=int(event.get("style_version")),
        request_type=event.get("type"),
        request_name=event.get("request_name"),
        sales_channel=event.get("sales_channel"),
        allocation_status=event.get("allocation_status"),
        ordered_at=event.get("line_item_creation_date"),
        order_name=event.get("order_number"),
        order_line_item_id=event.get("order_line_item_id"),
        channel_order_id=event.get("channel_order_id"),
        channel_line_item_id=event.get("channel_line_item_id"),
    )


# bumped by rob: 4 times..

if __name__ == "__main__":
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)
    fc = FlowContext.from_queue_name(KAFKA_JOB_EVENT_TOPIC)
    while True:
        try:
            # Get new messages.... ...
            raw_message = kafka_consumer.poll_avro(timeout=60)
            if raw_message is None:
                logger.info("kafka poll timed out, continuing........ ")
                continue
            logger.info("Found a new message!")
            fc.queue_entered_metric_inc()
            response = post_with_token(
                "https://data.resmagic.io/make-one/production_requests/create",
                secret_name="RES_META_ONE_API_KEY",
                # WTF.  this works in some cases where just saying data = thing.json() fails...
                json=kafka_message_to_make_request(raw_message).dict(),
            )
            response.raise_for_status()
            response_json = response.json()
            if (
                "status" not in response_json
                or response_json["status"] != "REQUEST_CREATED"
            ):
                raise Exception(
                    f"Got bad response with status: {response_json['status']}"
                )

            try:
                """
                SA: this seems the best place to do the check for some dropped orders in postgres
                the kafka consumers are suppose to feed orders directly in there as they come from shopify or create one
                but for any reason if they are dropped e.g. the webhook failed, the kafka relay failed etc
                we can here make sure we have the occasional missed message
                """
                logger.info(
                    "Checking for order in the postgres database - it should be there already but just in case..."
                )
                # bit of a hack - the request name happens to have the order channel but its otherwise not in the payload
                request_name_channel_prefix = (
                    raw_message.get("request_name").split(" ")[0].strip()
                )
                order_number = raw_message.get("order_number")
                pull_missing_order_if_not_exists(
                    order_number=order_number, channel=request_name_channel_prefix
                )
            except Exception as ex:
                logger.warn(f"Failed to do the order check - {ex}")

            fc.queue_exit_success_metric_inc()
        except Exception as e:
            logger.info(f"an error ocurred: {e}")
            err = f"Error processing event: {raw_message}. Error -> {traceback.format_exc()}"
            logger.critical(
                err,
                exception=e,
            )
            fc.queue_exception_metric_inc()
            ping_slack(
                f"""Failed to generate production request: ```{repr(e)}```
                with payload ```{raw_message}```""",
                "autobots",
            )
