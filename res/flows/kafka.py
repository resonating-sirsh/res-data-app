import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.flows.FlowContext import FlowContext
from res.utils import logger


def kafka_consumer_worker(event_topic: str):
    """
    A decorator that wraps a function and provides the functionality of a KafkaConsumerWorker.

    Example:

        @kafka_consumer_worker('path.to.event.topic')
        def handle_event(message):
            # do something with message

        handle_event()
    """

    def decorator(func: "function"):
        def wrapper(*args, **kwargs):
            kafka_consumer_worker = KafkaConsumerWorker(event_topic, func)
            kafka_consumer_worker.run()

        return wrapper

    return decorator


def _true():
    """This is a function that always returns True. We use it so we can stub out
    the loop for testing."""
    return True


class KafkaConsumerWorker:
    """ "
    A KafkaConsumerWorker is a class that handles the boilerplate code for consuming
    messages from a Kafka topic. While it can be used directly, it is intended to be
    used as part of a decorator.

    Example:

        def handle_event(message):
            # do something with message

        KafkaConsumerWorker('path.to.event.topic', handle_event).run()

    """

    def __init__(self, event_topic: str, event_handler: "function"):
        self.event_topic = event_topic
        self.event_handler = event_handler
        self.kafka_client = ResKafkaClient()
        self.kafka_consumer = ResKafkaConsumer(self.kafka_client, event_topic)
        self.fc = FlowContext.from_queue_name(event_topic)

    def run(self):
        while _true():
            try:
                message = self.kafka_consumer.poll_avro(timeout=60)
                if message is None:
                    logger.info("kafka poll timed out, continuing... ... ... ...")
                    continue
                logger.info(f"Found a new message! -> {message}")
                self.fc.queue_entered_metric_inc()
                self.event_handler(message)
                self.fc.queue_exit_success_metric_inc()
            except Exception as e:
                logger.critical(
                    f"Error processing event: {message}. Error -> {traceback.format_exc()}",
                    exception=e,
                )
                self.fc.queue_exception_metric_inc()
