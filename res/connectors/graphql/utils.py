from asyncio.log import logger


def from_hasura_event_payload(json_request):
    event = json_request["event"]
    metadata = event["session_variables"]
    operation = event["op"]
    logger.info(f"Event received event {metadata}, OPERATION: {operation}")
    logger.debug(event)
    changes = event["data"]
    old_changes = changes["old"]
    new_changes = changes["new"]
    logger.info(f"\nold: {old_changes}\n\nnew: {new_changes}")
    return new_changes, operation, old_changes


class HasuraEventType:
    UPDATE = "UPDATE"
    INSERT = "INSERT"
    DELETE = "DELETE"
