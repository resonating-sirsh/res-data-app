from res.utils import logger
import boto3, os

DEFAULT_KEY_SCHEMA = [{"AttributeName": "key", "KeyType": "HASH"}]
DEFAULT_ATTR_SCHEMA = [{"AttributeName": "key", "AttributeType": "S"}]
DEFAULT_THROUGHPUT = {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}


class ResDynamoClient:
    """
    Wrapper around boto3 Dynamo client.
    """

    def __init__(self):
        self._env = os.getenv("RES_ENV", "development")
        self.dynamo_client = boto3.client("dynamodb")
        self.dynamo_resource = boto3.resource("dynamodb")

    def create_table_if_not_exists(
        self,
        table_name,
        key_schema=DEFAULT_KEY_SCHEMA,
        value_schema=DEFAULT_ATTR_SCHEMA,
        throughput=DEFAULT_THROUGHPUT,
    ):
        try:
            logger.debug("Trying to create new dynamo table: {}".format(table_name))
            table = self.dynamo_resource.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=value_schema,
                ProvisionedThroughput=throughput,
            )
        except self.dynamo_client.exceptions.ResourceInUseException:
            logger.debug("Table exists!: {}".format(table_name))
            # Table exists, just get the table object and return
            table = self.dynamo_resource.Table(table_name)
        return table

    def get_items(self, table_name, lookup_value, lookup_key="key"):
        table = self.dynamo_resource.Table(table_name)
        items = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr(lookup_key).eq(lookup_value)
        )["Items"]
        return items

    def set_item(self, table_name, item):
        table = self.dynamo_resource.Table(table_name)
        table.put_item(Item=item)
