from distutils.command.config import config
import res


# Example update config: https://docs.pinot.apache.org/basics/data-import/upsert
# {
#   "upsertConfig": {
#     "mode": "PARTIAL",
#     "partialUpsertStrategies":{
#    #   not shown overwrite which is default
#       "rsvp_count": "INCREMENT", #add to values
#    #  the next two merge the unordered Pinot set
#       "group_name": "UNION",
#       "venue_name": "APPEND"
#     }
#   }
# }

# when compacting to ofline in UPSERT modes we also need to do this
# https://docs.pinot.apache.org/operators/operating-pinot/pinot-managed-offline-flows
# ^ ths is how we add tasks to the realtime table spec -> add this under "task"

# "taskTypeConfigsMap": {
#      "RealtimeToOfflineSegmentsTask": {
#        "bucketTimePeriod": "1d",
#        "bufferTimePeriod": "5d",
#        "mergeType": "dedup",
#        "maxNumRecordsPerSegment": "10000000"
#      }
# }

# how we map schema
# https://docs.pinot.apache.org/basics/components/schema
# https://docs.pinot.apache.org/configuration-reference/schema
def _map_type_from_avro(type):
    pass


def load_avro_schema(topic):
    kafka = res.connectors.load("kafka")[topic]
    schema = kafka.topic_schema
    fields = {}
    return fields


def realtime_table_create(
    table_name,
    schema_name=None,
    upsert_config=None,
    partial_update_spec=None,
    time_column="created_at",
    **override,
):
    """
    bunch of things are hard-coded here just to create a table spec from a topic
    there are lots of things we will want to configure when we know how
    also we will want to load the services from the environment e.g. kafka brokers

    - assume SECOND precision is all we want on all topics
    - by default we dont use an UPSERT table but we often pair CDC and Status (UPSERT) tables together via some key

    troubleshoot:
    java.lang.NullPointerException -> probably the kafka topic does not exist - we shoudl validate here
    """

    # TODO: update cluster URIs from env + other options - this is useful for hardcoded testing

    schema_name = schema_name or table_name

    # how we manage tables
    if upsert_config:
        table_name = f"{table_name}_stateful"
    table_name = table_name.replace(".", "_")

    res.utils.logger.info(f"Creating table {table_name}")

    spec = {
        # this is important - pinot does not allow tables to have spaces or dots but the schema does and same for kafka
        # so just when creating tables we map
        "tableName": table_name,
        "tableType": "REALTIME",
        "segmentsConfig": {
            "timeColumnName": time_column,
            # seconds are assumed for now as we work out schema
            "timeType": "SECONDS",
            "retentionTimeUnit": "SECONDS",
            "retentionTimeValue": "3650",
            "segmentPushType": "APPEND",
            "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy",
            "schemaName": schema_name,
            "replication": "1",
            "replicasPerPartition": "1",
        },
        "routing": {},
        "tenants": {},
        "tableIndexConfig": {
            "loadMode": "MMAP",
            "streamConfigs": {
                "streamType": "kafka",
                "security.protocol": "SSL",
                "ssl.keystore.location": "/usr/local/openjdk-11/lib/security/cacerts",
                "ssl.keystore.password": "changeit",
                "stream.kafka.topic.name": schema_name,
                # low level needed for things like upserts - see docs
                "stream.kafka.consumer.type": "lowlevel",
                "stream.kafka.consumer.prop.auto.offset.reset": "largest",
                "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
                "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder",
                "realtime.segment.flush.threshold.rows": "0",
                "realtime.segment.flush.threshold.time": "24h",
                "realtime.segment.flush.segment.size": "100M",
                "stream.kafka.zk.broker.url": "z-1.res-primary-dev.0hn5z0.c13.kafka.us-east-1.amazonaws.com:2181",
                "stream.kafka.broker.list": "b-3.res-primary-dev.0hn5z0.c13.kafka.us-east-1.amazonaws.com:9094,b-2.res-primary-dev.0hn5z0.c13.kafka.us-east-1.amazonaws.com:9094,b-1.res-primary-dev.0hn5z0.c13.kafka.us-east-1.amazonaws.com:9094",
                "schema.registry.url": "http://kafka-schema-registry-cp-schema-registry.kafka-schema-registry.svc.cluster.local:8081",
                "stream.kafka.decoder.prop.schema.registry.rest.url": "http://kafka-schema-registry-cp-schema-registry.kafka-schema-registry.svc.cluster.local:8081",
            },
        },
        "metadata": {"customConfigs": {}},
    }

    # update the spec with overrides

    if upsert_config:
        spec["routing"]["instanceSelectorType"] = "strictReplicaGroup"
        spec["upsertConfig"] = (
            {"mode": "FULL"} if not partial_update_spec else partial_update_spec
        )

    return spec
