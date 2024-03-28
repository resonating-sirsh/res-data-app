import os, uuid, json
from res.utils import logger
from res.connectors.dynamo.ResDynamoClient import ResDynamoClient


# Creates an example table if not exists, stores a record, then retrieves it
if __name__ == "__main__":
    table_name = os.getenv("TABLE_NAME", "example_table")
    message = os.getenv("MESSAGE", "hello world!")
    key = str(uuid.uuid4())
    dynamo_item = {"key": key, "message": message}

    client = ResDynamoClient()
    logger.info(f"Creating table {table_name}...")
    client.create_table_if_not_exists(table_name)
    logger.info(f"Setting item {json.dumps(message)}")
    client.set_item(table_name, dynamo_item)
    item = client.get_items(table_name, key)
    logger.info(f"Retrieved item {json.dumps(item)}")
