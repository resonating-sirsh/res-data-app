"""
This is a consumer for res_make.payment.request
It processes all make order requests for payments on stripe.
It sends a message to charge the subscription balance for the brand or set the was_balance_not_enough flag,
when the balance is not enough or when the brand is_direct_Payment_default is set
which gets processed on delayed_payment_processor cron job after.
"""
import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger
from res.flows.make.payment.request import handle_new_event

from res.flows.FlowContext import FlowContext

KAFKA_JOB_EVENT_TOPIC = "res_make.payment.request"
DEAD_LETTER_THRESHOLD = 10

if __name__ == "__main__":
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)
    fc = FlowContext.from_queue_name(KAFKA_JOB_EVENT_TOPIC)
    while True:
        try:
            # Get new messages... ...
            raw_message = kafka_consumer.poll_avro(timeout=60)
            if raw_message is None:
                logger.debug("kafka poll timed out, continuing....")
                continue
            logger.info("Found new message!! :)")
            fc.queue_entered_metric_inc()
            handle_new_event(raw_message)
            fc.queue_exit_success_metric_inc()

        except Exception as e:
            trace = traceback.format_exc()
            error = f"Error processing event: {raw_message}. Error -> {trace}"
            logger.info("Error")
            logger.warn(error)
            fc.queue_exception_metric_inc()
            # Send to dead letter with max of 10 attempt
            fc.write_dead_letter(
                KAFKA_JOB_EVENT_TOPIC,
                raw_message,
                exception_trace=trace,
                error_after_counter=DEAD_LETTER_THRESHOLD,
            )
