from res.flows.kafka import kafka_consumer_worker
from res.utils import logger


@kafka_consumer_worker("res_examples.kafka.consumer_worker")
def work(message):
    logger.debug(f"We've received a message! {message['message']}")


if __name__ == "__main__":
    work()
