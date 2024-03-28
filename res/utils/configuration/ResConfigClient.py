"""
This is a Resonance Configuration Client, which retrieves config values from a datastore. Currently the datastore is DynamoDB, but could be anything in the future

"""

import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from ..logging import logger

TABLE_PREFIX = "data_platform_configurations_{}"


class ResConfigClient:
    def __init__(self, environment=None, app_name=None, namespace=None):
        """
        Construct a client from the environment. Configs will be qualified env/namespace/app
        - Apps run in kubernetes with a namespace/app_name
        - Optionally override the namespace and app name to control the aliasing (e.g for flows)
        """
        self._environment = environment or os.getenv("RES_ENV", "development")
        self.dynamo_client = boto3.client("dynamodb")
        self.dynamo_resource = boto3.resource("dynamodb")
        self.table_name = TABLE_PREFIX.format(self._environment)
        self.namespace = namespace or os.getenv("RES_NAMESPACE", "testing")
        self.app_name = app_name or os.getenv("RES_APP_NAME", "testing")
        self.namespace_app_name = "{}_{}".format(self.namespace, self.app_name)
        # Try to create the table, catch error if already exists
        self.table = self.create_config_table()

    def get_config_value(self, config_name):
        full_config_name = "{}_{}".format(self.namespace_app_name, config_name)
        logger.debug("requesting config value: {}".format(full_config_name))
        response = self.table.get_item(
            Key={
                "config_name": full_config_name,
            }
        )
        logger.debug(response)
        if "Item" not in response:
            # Not set yet
            logger.warn(
                "Configuration requested, but not set yet: {}".format(full_config_name)
            )
            return None
        return response["Item"]["config_value"]

    def get_config_entries(self):
        """
        get key, value entries for app - removing the app qualified from the key
        """
        items = self.table.scan(
            FilterExpression=Attr("full_app_name").eq(self.namespace_app_name)
        )["Items"]

        return {
            item["config_name"].replace(f"{self.namespace_app_name}_", ""): item[
                "config_value"
            ]
            for item in items
        }

    def get_config_values(self):
        """
        Get all config values for an app (needs to have been saved with full_app_name or we must scan client side)
        """
        items = self.table.scan(
            FilterExpression=Attr("full_app_name").eq(self.namespace_app_name)
        )["Items"]

        return [item["config_value"] for item in items]

    def set_config_value(self, config_name, config_value):
        # Adds a new config if doesn't exist, updates existing if already exists
        full_config_name = "{}_{}".format(self.namespace_app_name, config_name)
        logger.debug("setting config value: {}".format(full_config_name))
        self.table.put_item(
            Item={
                "config_name": full_config_name,
                "config_value": config_value,
                "full_app_name": self.namespace_app_name,
            }
        )

    def create_config_table(self):
        try:
            logger.debug(
                "Trying to create new dynamo table: {}".format(self.table_name)
            )
            table = self.dynamo_resource.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {"AttributeName": "config_name", "KeyType": "HASH"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "config_name", "AttributeType": "S"},
                ],
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )
        except self.dynamo_client.exceptions.ResourceInUseException:
            logger.debug("Table exists!: {}".format(self.table_name))
            # Table exists, just get the table object and return
            table = self.dynamo_resource.Table(self.table_name)
        return table
