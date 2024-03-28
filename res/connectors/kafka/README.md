These classes wrap various Kafka functionality. In order of lowest-level to highest:

ResKafkaClient: intializes all variables for accessing Kafka, including bootstrap servers and schema registry URLs (among other things). Allows for utility/admin type actions against kafka, such as creating a topic.

ResKafkaConsumer: wraps consumer behavior, for retrieving messages from Kafka

ResKakfaProducer: wraps producer behavior, for sending messages to Kafka

ResKafkaIntegrations: used for creating "sinks" and "sources" via KafkaConnect. Useful for dumping messages to S3 or Snowflake, or syncing topics via CDC with Postgres DBs.

ResKafkaConnector: high-level connector for Kafka, utilizing data frames and simplified functions for consuming/producing messages