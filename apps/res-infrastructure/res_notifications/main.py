import traceback
import json
import re
from res.connectors.airtable.AirtableClient import ResAirtableClient
from res.utils import logger
from res.connectors.graphql.hasura import Client
from queries import INSERT_MESSAGE, GET_MESSAGE, UPDATE_MESSAGE


def handler():

    # Notifications table
    base_id = "appEm3sHF7BTTzgyq"
    table_id = "tbltRVfgzkzYNvf3q"

    HASURA_CLIENT = Client()
    # GET ALL NOTIFICATION FROM NOTIFICATION TABLE AIRTABLE
    live_notification_records = list(
        ResAirtableClient().get_records(
            base_id, table_id, filter_by_formula="{Live}=1", log_errors=False
        )
    )
    hasura_records = HASURA_CLIENT.execute(GET_MESSAGE)[
        "infraestructure_res_notifications"
    ]
    # if table id and record id exist in hasura
    for record in live_notification_records:
        fields = record.get("fields")
        try:
            # Build the message
            message_field = fields.get("Message")
            list_of_words = re.findall(r"(?<=\{).+?(?=\})", message_field)

            # Build the action url
            action_url = fields.get("Action URL")
            list_of_parameters = re.findall(r"(?<=\{).+?(?=\})", action_url)

            source_records = list(
                ResAirtableClient().get_records(
                    f"{fields.get('Base ID')}",
                    f"{fields.get('Table ID')}",
                    log_errors=False,
                )
            )
            field_name = f"{fields.get('Trigger Field')}"
            field_value = f"{fields.get('Trigger Field Value')}"

            for source_record in source_records:
                source_fields = source_record.get("fields")
                message = f"{fields.get('Message')}"
                action_link = f"{fields.get('Action URL')}"

                does_exist_in_hasura = any(
                    x["source_table_id"] == f"{fields.get('Table ID')}"
                    for x in hasura_records
                ) and any(
                    x["source_record_id"] == f"{source_record.get('id')}"
                    for x in hasura_records
                )

                if does_exist_in_hasura:
                    for word in list_of_words:
                        word_value = source_fields.get(f"{word}")
                        word_to_change = "{" + f"{word}" + "}"
                        if word_value:
                            message = message.replace(word_to_change, word_value)
                    for param in list_of_parameters:
                        param_value = source_fields.get(f"{param}")
                        param_to_change = "{" + f"{param}" + "}"
                        if param_value:
                            action_link = action_link.replace(
                                param_to_change, param_value
                            )
                    current_record_id = source_record.get("id")
                    current_hasura_record = next(
                        filter(
                            lambda x: x["source_record_id"] == current_record_id,
                            hasura_records,
                        )
                    )
                    try:
                        if (
                            source_fields.get(field_name) == field_value
                            and current_hasura_record["should_send"] is False
                        ):
                            # update should_send to true and if there are any other fields that needs updating update them as well
                            HASURA_DATA = HASURA_CLIENT.execute(
                                UPDATE_MESSAGE,
                                {
                                    "destination": source_fields.get(
                                        f"{fields.get('Recipients Code')}"
                                    ),
                                    "message": message,
                                    "source_table_id": fields.get("Table ID"),
                                    "read": False,
                                    "received": False,
                                    "topic": fields.get("Topic"),
                                    "channels": json.dumps([fields.get("Channel")]),
                                    "subchannel": fields.get("Subspace"),
                                    "links": json.dumps(action_link),
                                    "source_record_id": source_record.get("id"),
                                    "should_send": True,
                                },
                            )
                            logger.info(HASURA_DATA)
                        elif (
                            source_fields.get(field_name) != field_value
                            and current_hasura_record["should_send"] is True
                        ):
                            # create a new record in hasura with shoud_send as false
                            HASURA_DATA = HASURA_CLIENT.execute(
                                INSERT_MESSAGE,
                                {
                                    "destination": source_fields.get(
                                        f"{fields.get('Recipients Code')}"
                                    ),
                                    "message": message,
                                    "source_table_id": fields.get("Table ID"),
                                    "read": False,
                                    "received": False,
                                    "topic": fields.get("Topic"),
                                    "channels": json.dumps([fields.get("Channel")]),
                                    "subchannel": fields.get("Subspace"),
                                    "links": json.dumps(action_link),
                                    "source_record_id": source_record.get("id"),
                                    "should_send": False,
                                },
                            )
                            logger.info(HASURA_DATA)
                    except Exception as e:
                        err = "Error: {}".format(traceback.format_exc())
                        logger.critical(
                            err,
                            exception=e,
                        )
                elif source_fields.get(field_name) == field_value:
                    # create a new record in hasura with should send as true
                    for word in list_of_words:
                        word_value = source_fields.get(f"{word}")
                        word_to_change = "{" + f"{word}" + "}"
                        if word_value:
                            message = message.replace(word_to_change, word_value)
                    for param in list_of_parameters:
                        param_value = source_fields.get(f"{param}")
                        param_to_change = "{" + f"{param}" + "}"
                        if param_value:
                            action_link = action_link.replace(
                                param_to_change, param_value
                            )
                    HASURA_DATA = HASURA_CLIENT.execute(
                        INSERT_MESSAGE,
                        {
                            "destination": source_fields.get(
                                f"{fields.get('Recipients Code')}"
                            ),
                            "message": message,
                            "source_table_id": fields.get("Table ID"),
                            "read": False,
                            "received": False,
                            "topic": fields.get("Topic"),
                            "channels": json.dumps([fields.get("Channel")]),
                            "subchannel": fields.get("Subspace"),
                            "links": json.dumps(action_link),
                            "source_record_id": source_record.get("id"),
                            "should_send": True,
                        },
                    )
                    logger.info(HASURA_DATA)
        except Exception as e:
            err = "Error: {}".format(traceback.format_exc())
            logger.critical(
                err,
                exception=e,
            )


if __name__ == "__main__":
    handler()
