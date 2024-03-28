import traceback
import res
import re
import json
from res.utils import logger
from res.connectors.airtable.AirtableClient import ResAirtableClient
from res.connectors.graphql.hasura import Client
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient

HASURA_CLIENT = Client()
KAFKA_CLIENT = ResKafkaClient()

GET_NOTIFICATIONS = """
query GetNotifications($source_record_id: String! $source_table_id: String!) {
  infraestructure_res_notifications(where: {_and: {source_record_id: {_eq: $source_record_id}, source_table_id: {_eq: $source_table_id}}}) {
    source_record_id
    source_table_id
    should_send
    id
  }
}
"""

GET_BRAND = """
    query getBrand($code: String) {
        brand(code: $code){
            id
            slackChannel
        }
    }
"""

INSERT_MESSAGE = """
    mutation InsertNotifications($destination: String, $message: String, $source_table_id: String, $channels: json, $subchannel: String, $links: json, $topic: String, $read_at: timestamptz, $read: Boolean, $received: Boolean, $should_send: Boolean, $source_record_id: String) {
        insert_infraestructure_res_notifications(objects: {destination: $destination, message: $message, source_table_id: $source_table_id, channels: $channels, subchannel: $subchannel, links: $links, topic: $topic, read_at: $read_at, read: $read, received: $received, should_send: $should_send, source_record_id: $source_record_id}) {
            affected_rows
        }
    }
"""

UPDATE_MESSAGE = """
    mutation UpdateNotification($id: uuid, $destination: String, $message: String, $source_table_id: String, $channels: json, $subchannel: String, $links: json, $topic: String, $read_at: timestamptz, $read: Boolean, $received: Boolean, $should_send: Boolean, $source_record_id: String) {
        update_infraestructure_res_notifications(where: {id: {_eq: $id}}, _set: {destination: $destination, message: $message, source_table_id: $source_table_id, channels: $channels, subchannel: $subchannel, links: $links, topic: $topic, read_at: $read_at, read: $read, received: $received, should_send: $should_send, source_record_id: $source_record_id}) {
            affected_rows
        }
    }
"""


def get_field_type(base_id, table_id, field_name):
    records = list(
        ResAirtableClient().get_records(
            base_id,
            table_id,
            limit=1,
            get_all=False,
            filter_by_formula=f"AND({{{field_name}}}!='')",
        )
    )
    fields = records[0]["fields"]
    field_type = str(type(fields[field_name]))
    return field_type.split("'")[1]


def get_brand(brand_code):
    graphql_client = ResGraphQLClient()
    return graphql_client.query(GET_BRAND, {"code": brand_code})["data"]["brand"]


def extract_destinations(destinations, notification_type):
    identifiers = {"slack": "#"}
    destinations_list = []
    for word in destinations:
        if word.startswith(identifiers[notification_type]):
            item = word[1:].strip()
            if item:
                destinations_list.append(item)
    return destinations_list


def manage_slack_notification(payload, action_link, defualt_destinations):
    try:
        brand = get_brand(payload["destination"])
        if defualt_destinations:
            channels = extract_destinations(defualt_destinations, "slack") + [
                brand.get("slackChannel")
            ]
        else:
            channels = [brand.get("slackChannel")]
        message = f"""*{payload['message']}*\n
        👉 `<{action_link}|REVIEW>`
           """
        slack_message = {
            "slack_channels": channels,
            "message": message,
            "attachments": [],
        }
        logger.info("Starting to process slack notifications.")
        slack = res.connectors.load("slack")
        slack(slack_message)
    except Exception as e:
        err = "Error notifying in Slack Channel: {}".format(traceback.format_exc())
        print(err)
        logger.critical(
            err,
            exception=e,
        )


def handler(event, context={}):
    # Notifications table
    logger.info("Starting process...")
    base_id = "appEm3sHF7BTTzgyq"
    table_id = "tbltRVfgzkzYNvf3q"

    # GET ALL NOTIFICATION FROM NOTIFICATION TABLE AIRTABLE
    live_notification_records = list(
        ResAirtableClient().get_records(
            base_id,
            table_id,
            filter_by_formula="AND({Live}=1)",
            log_errors=False,
        )
    )

    # if table id and record id exist in hasura
    for record in live_notification_records:
        fields = record.get("fields")
        try:
            launch_date = fields.get("Launch Date")
            channels = fields.get("Channels")
            list_of_field_names = fields.get("Trigger Field").split(",")
            list_of_field_values = fields.get("Trigger Field Value").split(",")
            brand_code_field_name = fields.get("Brand Code Field")
            default_recipients = fields.get("Default Recipients")

            value_mapping = {"True": 1, "False": 0}
            fields_type = {}
            for field in list_of_field_names:
                fields_type[field] = get_field_type(
                    fields.get("Base ID"),
                    fields.get("Table ID"),
                    field,
                )

            filter_fields = []
            for trigger_field, trigger_value in zip(
                list_of_field_names, list_of_field_values
            ):
                # Comparing last updated to empty as well since some fields don't change since creation and the updated at it remains empty.
                filter_fields.append(
                    f"OR(LAST_MODIFIED_TIME({{{trigger_field}}}) > '{launch_date}', LAST_MODIFIED_TIME({{{trigger_field}}})='')"
                )
                if fields_type.get(trigger_field) == "list":
                    filter_fields.append(
                        f"FIND('{value_mapping.get(trigger_value, trigger_value)}', ARRAYJOIN({{{trigger_field}}})) > 0"
                    )
                else:
                    value = (
                        value_mapping.get(trigger_value)
                        if trigger_value in value_mapping
                        else f"'{trigger_value}'"
                    )
                    filter_fields.append(f"{{{trigger_field}}}={value}")
            filter_formula_string = "AND(" + ",".join(filter_fields) + ")"

            source_records = list(
                ResAirtableClient().get_records(
                    fields.get("Base ID"),
                    fields.get("Table ID"),
                    filter_by_formula=filter_formula_string,
                    log_errors=False,
                )
            )

            for source_record in source_records:
                hasura_records = HASURA_CLIENT.execute(
                    GET_NOTIFICATIONS,
                    {
                        "source_record_id": source_record.get("id"),
                        "source_table_id": fields.get("Table ID"),
                    },
                )["infraestructure_res_notifications"]
                source_fields = source_record.get("fields")
                message = f"{fields.get('Message')}"
                title = f"{fields.get('Topic')}"
                action_link = f"{fields.get('Action URL')}"
                brand_code = source_fields[brand_code_field_name]

                does_exist_in_hasura = len(hasura_records) > 0

                is_triggered = False

                for trigger_field, current_value in zip(
                    list_of_field_names, list_of_field_values
                ):
                    source_field_value = source_fields.get(trigger_field)
                    if current_value in {"True", "False"}:
                        current_value = current_value == "True"
                    if source_field_value == current_value:
                        is_triggered = True
                    elif isinstance(source_field_value, list):
                        is_triggered = current_value in source_field_value
                    else:
                        is_triggered = False
                        continue
                target_strings = {
                    "message": message,
                    "action_link": action_link,
                    "title": title,
                }

                for variable_name, target_string in target_strings.items():
                    new_string = target_string
                    for word in re.findall(r"{(.*?)}", target_string):
                        if word in source_fields:
                            new_string = new_string.replace(
                                "{" + word + "}", source_fields[word]
                            )
                    target_strings[variable_name] = new_string

                message = target_strings["message"]
                action_link = target_strings["action_link"]
                title = target_strings["title"]

                if does_exist_in_hasura:
                    current_hasura_record = hasura_records[0]
                    action = "update"

                    if (
                        is_triggered
                        and current_hasura_record
                        and current_hasura_record["should_send"] is False
                    ):
                        should_send_notification = "Create One" in channels
                    else:
                        continue
                elif is_triggered:
                    action = "insert"
                    should_send_notification = "Create One" in channels

                else:
                    continue

                payload = {
                    "destination": brand_code,
                    "message": message,
                    "source_table_id": fields.get("Table ID"),
                    "topic": title,
                    "channels": json.dumps(channels),
                    "subchannel": fields.get("Subspace", ""),
                    "source_record_id": source_record.get("id"),
                    "should_send": should_send_notification,
                    "read": False,
                    "received": False,
                    "links": json.dumps(action_link),
                }

                if action == "update":
                    payload["id"] = current_hasura_record["id"]

                HASURA_CLIENT.execute(
                    UPDATE_MESSAGE if action == "update" else INSERT_MESSAGE, payload
                )

                if "Slack" in channels and action == "insert":
                    manage_slack_notification(payload, action_link, default_recipients)

        except Exception as e:
            err = "Error generating new notification: {}".format(traceback.format_exc())
            print(err)
            logger.critical(
                err,
                exception=e,
            )


def on_success(event, context):
    pass


def on_failure(event, context):
    pass


if __name__ == "__main__":
    handler({}, {})
