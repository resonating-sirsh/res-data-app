import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger, ping_slack, post_with_token
from res.flows import FlowContext

KAFKA_JOB_EVENT_TOPIC = "res_make.make_one_request.reproduce"


if __name__ == "__main__":
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)
    fc = FlowContext.from_queue_name(KAFKA_JOB_EVENT_TOPIC)

    while True:
        try:
            # Get new messages
            raw_message = kafka_consumer.poll_avro(timeout=60)
            if raw_message is None:
                logger.info("kafka poll timed out, continuing..........")
                continue
            logger.info("Found a new message! :)")
            fc.queue_entered_metric_inc()
            post_with_token(
                "https://data.resmagic.io/make-one/production_requests/reproduce",
                secret_name="RES_META_ONE_API_KEY",
                json={
                    "reproduced_request_id": raw_message["request_id"],
                },
            ).raise_for_status()
            fc.queue_exit_success_metric_inc()
            ping_slack(
                f"Reproduced {raw_message['request_id']} :oof2: https://open.spotify.com/track/284bgvG8Atv3yBusws7ST3?si=28e8e3851f004a89",
                "autobots",
            )

        except Exception as e:
            logger.warning(f"Event: {raw_message} failed.")
            print(f"{e}")
            err = f"Error processing event: {raw_message}. Error -> {traceback.format_exc()}"
            logger.critical(
                err,
                exception=e,
            )
            fc.queue_exception_metric_inc()
            ping_slack(
                f"Failed to reproduce: {repr(e)} with payload: ```{raw_message}```",
                "autobots",
            )
