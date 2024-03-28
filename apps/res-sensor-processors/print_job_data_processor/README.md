### Print Job Data Processor.

This is a long-running process that consumes+produces data from Kafka.

Data is sent from a process running in the factory, which gets XML data from the Printers and sends to KGateway, Kafka topic "printJobData".

This process picks up that data, extracts the JobID, finds the right record_id in the Airtable res.Magic.Print/Nests table, and sends a new message to the Kafka>Airtable topic ("res-infrastructure.kafka_to_airtable.airtable_updates")

Then another process (kakfa-to-airtable) picks up that message and makes the corresponding change in Airtable.

To gen requirements (this just uses the standard reqs):

pip-compile .requirements/core-requirements.in -o apps/res-sensor-processors/print_job_data_processor/requirements.txt

bumping the right app
