from print_job_data_transformer import PrintJobDataTransformer

from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.utils import logger
import traceback

KAFKA_INPUT_TOPIC = "res_premises.printer_data_collector.print_job_data"
KAFKA_AIRTABLE_TOPIC = (
    "res_infrastructure.kafka_to_airtable.airtable_multifields_update"
)
KAKFA_SNOWFLAKE_TOPIC = ""


def run(kafka_client, kafka_consumer, transformer: PrintJobDataTransformer):
    with ResKafkaProducer(
        kafka_client,
        KAFKA_AIRTABLE_TOPIC,
    ) as kafka_producer:
        # Start consumer loop
        while True:
            try:
                # Get new messages
                raw_message = kafka_consumer.poll_avro()
                if raw_message is None:
                    logger.info("kafka poll timed out, continuing...")
                    continue
                logger.info("Found new message!")
                logger.debug("{}".format(str(raw_message)))

                raw_data = raw_message["MSjobinfo"]
                job_details = {
                    "requested_length": int(raw_data["RequestedLength"]),
                    "printed_length": int(raw_data["PrintedLength"]),
                    "media": raw_data["Media"],
                    "Print Mode": raw_data["PrintMode"],
                    "status_code": raw_data["Status"],
                    "Print Result": transformer.normalize_result(raw_data),
                }
                machine_name = raw_data["Printer"]
                status = transformer.normalize_status(raw_data)
                if status is None:
                    continue
                try:
                    print_file = transformer.get_print_file(raw_data)
                except Exception as e:
                    job_name = raw_data["Name"]
                    status_code = raw_data["Status"]

                    logger.error(
                        f"Error while making call to Airtable for Printjob: '{job_name}' ({status_code}: {status})",
                        job_name=job_name,
                        status=status,
                        status_code=status_code,
                        exception=e,
                    )
                    logger.debug(
                        f"Raw printjob data for failed Airtable call for Printjob: '{job_name}' ({status_code}: {status})",
                        job_name=job_name,
                        status=status,
                        status_code=status_code,
                        raw_data=raw_data,
                    )
                    continue
                logger.debug(print_file)
                if not print_file or "id" not in print_file:
                    continue

                # Send to Airtable Topic to update PrintJob table
                produced = False
                for payload in transformer.update_payload_generator(
                    status,
                    print_file,
                    job_details,
                    machine_name,
                ):
                    logger.debug(payload)
                    kafka_producer.produce(payload)
                    logger.info("Sent to airtable topic")
                    produced = True

                if not produced:
                    logger.warn(
                        "Invalid Message! Nest ID may not exist for job {}, or status is not in [0 through 5]".format(
                            raw_message["MSjobinfo"]["Name"]
                        )
                    )

                # Send to Snowflake Topic to add to IAC
                # snowflake_payload = transformer.get_kafka_snowflake_payload()
                # kafka_client.produce(KAKFA_SNOWFLAKE_TOPIC, snowflake_payload)
                # logger.info("Sent to snowflake topic")

            except Exception as e:
                err = "Error processing: {}".format(traceback.format_exc())
                logger.critical(
                    err,
                    exception=e,
                )


if __name__ == "__main__":
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_INPUT_TOPIC)
    transformer = PrintJobDataTransformer()
    run(kafka_client, kafka_consumer, transformer)
