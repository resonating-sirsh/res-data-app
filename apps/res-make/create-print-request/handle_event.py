import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger, ping_slack, post_with_token
from res.flows.FlowContext import FlowContext

#
KAFKA_JOB_EVENT_TOPIC = "res_make.print_request.create"

if __name__ == "__main__":
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)
    fc = FlowContext.from_queue_name(KAFKA_JOB_EVENT_TOPIC)
    while True:
        try:
            # Get new messages... ..
            raw_message = kafka_consumer.poll_avro(timeout=60)
            if raw_message is None:
                logger.debug("kafka poll timed out, continuing.... ")
                continue
            logger.debug("Found new message!! :)")
            fc.queue_entered_metric_inc()
            one_number = raw_message["order_number"]
            response = post_with_token(
                f"https://data.resmagic.io/make-one/print_assets/{one_number}",
                secret_name="RES_META_ONE_API_KEY",
            )
            response.raise_for_status()
            response_json = response.json()
            if "status" not in response_json:
                raise Exception(f"Got a bad response: {response_json}")
            if response_json["status"] != "CREATED":
                raise Exception(f"Got response with status: {response_json['status']}")
            fc.queue_exit_success_metric_inc()
            ping_slack(
                f"Created print assets for {one_number} ({response_json['sku']})",
                "autobots",
            )

        except Exception as e:
            print(f"{e}")
            err = f"Error processing event: {raw_message}. Error -> {traceback.format_exc()}"
            logger.critical(
                err,
                exception=e,
            )
            fc.queue_exception_metric_inc()
            ping_slack(
                f"""Failed to generate print request: ```{traceback.format_exc()}```
                with payload: ```{raw_message}```""",
                "autobots",
            )
