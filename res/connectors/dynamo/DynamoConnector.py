"""
This connector is currently a slightly strange pattern as it just wraps specific tables for specific purposes
Dyanmo is probably an intermedia solution so we are not investing too heavily in it
"""

from numpy.core import records
from res.utils import dates, logger
import numpy as np
import pandas as pd

from decimal import Decimal
import json
from itertools import islice
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Attr, Key, And
from dateparser import parse as date_parse
from . import DatabaseConnector, DatabaseConnectorTable, DatabaseConnectorSchema

from functools import reduce

#####
#
#   the dynamo db mapping used here is experimental and a little unusual
#   dynamo does not have schema. We are using big tables as schema.
#   therefore, tables in dynamo db = schema in our langauge
#   table is then used as a secondary index in the table
#   for example, we have airtable_cell as a dynamo db table which we will a schema
#   and then each actual airtable table_id maps to what we call table
#   that we we can say connector["airtable_cells"]["table_id"]
#   or connector['kafka-topics']['topic']
#   the secondary index is thus like a table. there is a cost to creating secondary index
#   we will return the underlying clients for users to do other things


# Learnings: we will refactor this into a simpler single provider. It evolved like this
# as we were trying different use cases for the KV store and each class when its own direction
# this is obviously wrong and will be refactored.
#  1. make sure floats, nans and other types that are not supported are cast
#  2. batch insertion and batch chunk reads
#


def chunk(it, size):
    "pagination tool"
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


class DynamoConnector(DatabaseConnector):
    """
    TODO: use the connector to index into environments
    """

    def __init__(self):
        pass

    def __getitem__(self, key):
        return DynamoConnectorSchema(schema_id=key)


class DynamoConnectorSchema(DatabaseConnectorSchema):
    """
    USe the schema to index into tables
    """

    def __init__(self, schema_id):
        self._schema_id = schema_id

    def __getitem__(self, key):
        return DynamoTable(table_id=key, schema_id=self._schema_id)


class DynamoTable(DatabaseConnectorTable):
    """
    This table class will merge learning from the case specific ones into a common interface
    They were fleshed out separately to iterate quickly without breaking changes
    But the pattern has emerged: mostly these functions wrap dynamos unfriendly client interfaces
    By assuming dataframes we can simplify the interface

    It  will be necessary for each table to define its key for operations.
    table = connector['development']['my_table'].set_key(list_key_fields) <- key can easily be composite for KV stores
    If the schema is present for the type we can load the key name behind the scenes
    The convention might be to always use an _id field in dynamo as a string of the key field(s)

    """

    def __init__(self, table_id, schema_id):
        self._table_id = table_id
        self._schema_id = schema_id
        self._dynamodb = boto3.resource("dynamodb")

        self._table_ref = self._dynamodb.Table(self._table_id)
        self._environment = None

    def create(self, secondary_index=None):
        # create the table for the environment and table id or name
        # always add an _id field
        # todo how to create the on-demand profile
        pass

    def _key_from_record(self, record):
        # default - overide with another attribute for the key
        return record["key"]

    @staticmethod
    def convert_floats(obj):
        """
        deep Conversion of floats
        """

        def f(c):
            if isinstance(c, float):
                return Decimal(str(np.round(c, 3)))

            if isinstance(c, dict) or isinstance(c, list):
                # recurse
                return DynamoTable.convert_floats(c)

            if pd.isnull(c):
                return None

            return c

        if isinstance(obj, list):
            return [{k: f(v) for k, v in d.items()} for d in obj]

        elif isinstance(obj, dict):
            return {k: f(v) for k, v in obj.items()}

        elif isinstance(obj, float):
            return Decimal(str(np.round(obj, 3)))

        if pd.isnull(obj):
            return None

        return obj

    def search(self, index_name="meta_one_bodies", index_key="body_code", **kwargs):
        if len([v for k, v in kwargs.items() if not pd.isnull(v)]) == 0:
            logger.warn("No predicates specified - returning the full table")
            return self.read()

        if "key" in kwargs and len(kwargs) == 1:
            return self.get(kwargs["key"])

        first_key = list(kwargs.keys())[0]
        logger.debug(f"filtering by first key {first_key} and other keys {kwargs}")

        # other query conditions
        conditions = [
            Attr(k).eq(v)
            for k, v in kwargs.items()
            if not pd.isnull(v) and k not in first_key
        ]

        conditions = reduce(And, conditions) if conditions else ""

        data = []

        response = self._table_ref.query(
            # FilterExpression=conditions,
            IndexName=index_name,
            KeyConditionExpression=Key(index_key).eq(kwargs[first_key]),
        )
        items = response.get("Items")
        if items:
            data.append(pd.DataFrame([i for i in items]))
        else:
            print("nothing found")
            return pd.DataFrame()

        while "LastEvaluatedKey" in response:
            response = self._table_ref.query(
                # FilterExpression=conditions,
                ExclusiveStartKey=response["LastEvaluatedKey"],
                IndexName=index_name,
                KeyConditionExpression=Key(index_key).eq(kwargs[first_key]),
            )
            items = response.get("Items")
            if len(items) > 0:
                data.append(pd.DataFrame([i for i in items]))

        data = pd.concat(data)
        for k, v in kwargs.items():
            if k in first_key:
                continue
            if v is not None and k in data.columns:
                data = data[data[k] == v]

        return data

    def put(self, record, convert_floats=True):
        """ """
        if convert_floats:
            record = DynamoTable.convert_floats(record)

        record["_id"] = self._key_from_record(record)
        self._table_ref.put_item(Item=record)

    def put_batch(self, records, check_exists=False):
        """ """

        if isinstance(records, dict):
            records = [records]
        elif isinstance(records, pd.DataFrame):
            records = records.where(pd.notnull(records), None)
            records = records.to_dict("records")

        with self._table_ref.batch_writer(
            overwrite_by_pkeys=[
                "_id",
            ]
        ) as batch:
            for record in records:
                record["_id"] = self._key_from_record(record)
                record["timestamp"] = dates.utc_now_iso_string()
                batch.put_item(Item=record)

    def to_dataframe(
        self, pivot=False, pivot_columns=["record_id", "column_name", "cell_value"]
    ):
        assert (
            self._table_ref is not None
        ), "To call get data you must specify a table name for DynamoDBTableMirror"
        response = self._table_ref.query(
            # This is the secondary index created on AWS
            IndexName="table_id-index",
            KeyConditionExpression=Key("table_id").eq(self._table_id),
        )

        data = pd.DataFrame(i for i in response["Items"])
        return data if not pivot else data.pivot(*pivot_columns)

    def read(self):
        return pd.concat(self.read_chunks()).reset_index().drop("index", 1)

    def read_chunks(self):
        response = self._table_ref.scan()
        items = response.get("Items")
        if items:
            last = response.get("LastEvaluatedKey")
            yield pd.DataFrame([i for i in items])

        while last:
            response = self._table_ref.scan(
                ExclusiveStartKey=last,
            )
            items = response.get("Items")
            last = response.get("LastEvaluatedKey")
            if len(items) > 0:
                yield pd.DataFrame([i for i in items])

    def get(self, key, item_as_dataframe=False):
        """
        We should always return a dataframe, will refactor the dynamo connectors and everything that uses them at some point
        """
        if isinstance(key, list):
            dfs = []
            for c in chunk(key, 100):
                batch_keys = {
                    self._table_id: {"Keys": [{"_id": k} for k in c]},
                }
                response = self._dynamodb.batch_get_item(RequestItems=batch_keys)
                df = pd.DataFrame(response["Responses"][self._table_id])
                dfs.append(df)
            return pd.concat(dfs).reset_index().drop("index", 1)

        else:
            response = self._table_ref.query(KeyConditionExpression=Key("_id").eq(key))
            if response["Count"] > 0:
                item = response["Items"][0]
                return item if item_as_dataframe else pd.DataFrame([item])

            return None


class EntityTypesDynamoTable(DynamoTable):
    # namespace/table/name {   'python', 'pandas', 'avro', 'looker', 'graphql', 'dgraph', 'is_nullable', 'is_key', 'broadcast_fields', 'stages'}]
    # derived tables are lists that we maintain where the field is present on a derived special table. These can be used to genreate topics for example - graph rep uses node edges and expands this out
    # broadcast fields take the entire row and flatten on a field e.g. some fields are lists for which we want entire events
    pass


class KafkaTopicDynamoTable(DynamoTable):
    def __init__(
        self,
        table_id="kafka_topic_keys",
        schema_id=None,
    ):
        self._table_id = table_id
        self._dynamodb = boto3.resource("dynamodb")
        self._table_ref = self._dynamodb.Table(self._table_id)

    def message_count(self, topic, index_name="index_topic"):
        scan = self._table_ref.query(
            IndexName=index_name,
            KeyConditionExpression=Key("topic").eq(topic),
        )
        return scan["Count"]

    def try_add_new_key(self, topic, key):
        key = f"{topic}/{key}"
        record = {
            "key": key,
            "created_at": datetime.utcnow().isoformat(),
            "topic": topic,
        }

        response = self._table_ref.query(KeyConditionExpression=Key("key").eq(key))
        if response["Count"] > 0:
            return False

        self._table_ref.put_item(Item=record)

        return True

    def _get_existing_keys(self, messages, key_field):
        dynamodb = boto3.resource("dynamodb")
        keys = list(messages[key_field].unique())
        items = []
        # dynamo allows 100 keys to be requested at a time
        for c in chunk(records, 100):
            batch_keys = {
                "data": {"Keys": [{"key": self._key_from_record(r)} for r in c]},
            }
            response = dynamodb.batch_get_item(RequestItems=batch_keys)
            existing = response["Responses"]["data"]

            for i in existing:
                items.append(i["key"])

        return items

    def drop_topic(self, topic, index_name="index_topic"):
        """
        scan batches and delete
        """
        count = -1
        while count != 0:
            self._drop_topic(topic, index_name)
            count = self.message_count(topic, index_name)

    def _drop_topic(self, topic, index_name="index_topic"):
        # TODO there is a scan limit actually so need to keep checking TODO:
        scan = self._table_ref.query(
            IndexName=index_name,
            KeyConditionExpression=Key("topic").eq(topic),
        )
        # i think this is redundant but better make sure
        # KeyConditionExpression=Key("topic").eq(topic),

        logger.info(f"purging topic {topic}")
        counter = 0
        with self._table_ref.batch_writer() as batch:
            for item in scan["Items"]:
                counter += 1
                batch.delete_item(
                    # if we have indexed in date and key??
                    Key={"key": item["key"], "created_at": item["created_at"]}
                )

        logger.info(f"Deleted {counter} records to purge the topic {topic}")


class ProductionStatusDynamoTable(DynamoTable):
    """ """

    def __init__(self, table_id="production_requests", schema_id=None):
        self._table_id = table_id
        self._dynamodb = boto3.resource("dynamodb")
        self._table_ref = self._dynamodb.Table(self._table_id)

    def update_item(self, record):
        """
        Rapid iteration produes these methods in each of these case specific classes
        As the pattern emerges, we merge these into the DynamoTable and deprecate the case specific tables

        #https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SQLtoNoSQL.UpdateData.html
        #nested cases https://stackoverflow.com/questions/35731270/is-it-possible-to-upsert-nested-fields-in-dynamodb

        """
        keys = list(record.keys())
        params = [f":v{i}" for i in range(len(keys))]
        ue = ",".join([f"{keys[i]}={params[i]}" for i in range(len(keys))])
        # print(ue)
        uv = {params[i]: record[keys[i]] for i in range(len(keys))}

        try:
            self._table_ref.update_item(
                Key={"_id": record["one_number"]},
                UpdateExpression=f"SET {ue}",
                ExpressionAttributeValues=uv,
            )
        except Exception as ex:
            print(f"Failed to write {record} to dyamo:{repr(ex)}")

    def update(self, dataframe, batch=True):
        """
        This update should roughly match some format based on Make.One.Production requests (tblptyuWfGEUJWwKk)
        """
        logger.info(f"updating {len(dataframe)} records for production status")
        if len(dataframe) > 0:
            logger.info(f"sample record {dataframe.iloc[0]}")
        dataframe["_id"] = dataframe["one_number"]
        with self._table_ref.batch_writer(overwrite_by_pkeys=["_id"]) as batch:
            for record in dataframe.to_dict("records"):
                for k, v in record.items():
                    # just store string values
                    record[k] = str(v)
                # record["created_at"] = record["created_at"].isoformat()
                batch.put_item(Item=record)

        logger.info("Done")

    def read(self):
        return pd.concat(self.read_chunks()).reset_index(drop=True)

    def read_chunks(self):
        response = self._table_ref.scan()
        items = response.get("Items")
        if items:
            last = response.get("LastEvaluatedKey")
            yield pd.DataFrame([i for i in items])

        while last:
            response = self._table_ref.scan(
                ExclusiveStartKey=last,
            )
            items = response.get("Items")
            last = response.get("LastEvaluatedKey")
            if len(items) > 0:
                yield pd.DataFrame([i for i in items])

    def get(self, key, item_as_dataframe=False):
        """
        We should always return a dataframe, will refactor the dynamo connectors and everything that uses them at some point
        """
        if isinstance(key, list):
            dfs = []
            for c in chunk(key, 100):
                batch_keys = {
                    "production_requests": {"Keys": [{"_id": k} for k in c]},
                }
                response = self._dynamodb.batch_get_item(RequestItems=batch_keys)
                df = pd.DataFrame(response["Responses"]["production_requests"])
                dfs.append(df)
            return pd.concat(dfs).reset_index().drop("index", 1)

        else:
            response = self._table_ref.query(KeyConditionExpression=Key("_id").eq(key))
            if response["Count"] > 0:
                item = response["Items"][0]
                return item if item_as_dataframe else pd.DataFrame([item])

            return None

    def update_roll_assignments(self, data):
        keys = list(data["one_number"].unique())

        logger.debug(f"Updating {len(keys)} unique ONEs for roll assignments")
        logger.debug("sample  roll assignment record")
        logger.debug(data.iloc[0])

        # this can be one-many - create a list
        lu = {}

        # we expect the roll ids to be lists but sometimes they may be individual items
        def try_roll_ids(s):
            if s is None or s == "No Roll Available for this Material":
                return []
            if not isinstance(s, list):
                return [s]
            return s

        data["roll_id"] = data["roll_id"].map(try_roll_ids)

        for o, r in data[["one_number", "roll_id"]].groupby("one_number"):
            # merging a list of lists
            l = []
            for _l in r["roll_id"]:
                if _l:
                    l += _l

            lu[o] = l or None

        for c in chunk(keys, 100):
            batch_keys = {
                "production_requests": {"Keys": [{"_id": k} for k in c]},
            }
            response = self._dynamodb.batch_get_item(RequestItems=batch_keys)
            df = pd.DataFrame(response["Responses"]["production_requests"])

            if len(df) > 0:
                # the one number is the id in the prod requests
                # if there are no records in prod requests already something else is wrong
                df["roll_id"] = df["_id"].map(lambda x: lu.get(x))
                self.update(df)

    def update_one(self, record):
        self._table_ref.put_item(Item=record)

    # def get_shopify_order(self, order_id):
    #     response = self._table_ref.scan(FilterExpression=Key("order_key").eq(order_id))
    #     return pd.DataFrame(i for i in response["Items"])

    @staticmethod
    def _lookup_production_orders(key=None):
        """
        because the airtable production orders do not properly store foreign keys to shopify orders
        they have to be looked up in a messy way to store the proper production request with shopify keys
        these keys can then be matched for streaming orders from shopify
        """
        from res.connectors.airtable import AirtableConnector

        def extract(x):
            try:
                res = x[0].replace("[", "").replace("]", "").split("-")[-1]
                if res == "":
                    return None
                return res
            except:
                return None

        def int_str(x):
            if str(x) == "":
                return None
            try:
                return str(int(float(x)))
            except:
                return None

        at = AirtableConnector()
        ORDER_ITEM_TABLE = "tblUcI0VyLs7070yI"
        ORDER_ITEM_BASE = "appfaTObyfrmPHvHc"

        fields = ["lineitem_id", "__order_unique_identifier"]
        if key == None:
            oi_lookup = at[ORDER_ITEM_BASE][ORDER_ITEM_TABLE].to_dataframe(
                fields=fields
            )
        else:
            oi_lookup = at[ORDER_ITEM_BASE][ORDER_ITEM_TABLE].to_dataframe(
                fields=fields,
                filters="FIND('" + key + "', {_record})",
            )

        oi_lookup["order_id"] = oi_lookup["__order_unique_identifier"].map(extract)
        oi_lookup["order_item_id"] = oi_lookup["lineitem_id"].map(int_str)
        oi_lookup = oi_lookup[
            (oi_lookup["order_id"].notnull()) & (oi_lookup["order_item_id"].notnull())
        ]

        return oi_lookup


class ShopifyOrdersDynamoTable(DynamoTable):
    def __init__(self, table_id="shopify_orders", schema_id=None):
        self._table_id = table_id
        self._dynamodb = boto3.resource("dynamodb")
        self._table_ref = self._dynamodb.Table(self._table_id)

    def _key_from_record(self, value):
        # working with this convention
        # return f"{value['id']}"
        # the shopify id does not seem to be the thing to track a user order
        if "name" not in value:
            raise Exception(
                f"record did not have the naeme key in {list(value.keys())}"
            )
        return f"{value['brand']}/{value['name']}"

    def _put_record(self, record):
        """
        wrapper around table write with some casting and other checks
        """
        record["_id"] = self._key_from_record(record)
        self._table_ref.put_item(Item=record)

    def _yield_by_keys(self, keys):
        for c in chunk(keys, 100):
            batch_keys = {
                "shopify_orders": {"Keys": [{"_id": k} for k in c]},
            }
            response = self._dynamodb.batch_get_item(RequestItems=batch_keys)
            yield pd.DataFrame(response["Responses"]["shopify_orders"])

    def read_by_keys(self, keys):
        """
        given a list of keys, make paged requests to dynamo to fetch the data and return a dataframe
        because we are dealing with small data we do not use a generator
        """

        return pd.concat(list(self._yield_by_keys(keys))).reset_index().drop("index", 1)

    def update_order_status(self, brand_code, order_name, status, ignore_missing=False):
        key = f"{brand_code}/{order_name}"

        existing_record = self._table_ref.get_item(Key={"_id": key})
        existing_record = (
            None if "Item" not in existing_record else existing_record["Item"]
        )

        # normally we assume the order exists here or we ignore
        # when we have a reliable schema on the stream we can always upsert without first checking exists
        if not existing_record:
            message = f"The order {key} is not in the store and cannot be updated"
            if ignore_missing:
                logger.warn(message)
                return
            raise Exception(message)

        if status not in ["open", "cancelled", "fulfilled"]:
            raise ValueError(f"The status {status} is not a valid order status")

        existing_record["status"] = status
        self._put_record(existing_record)

    def _get_record(self, key):
        try:
            response = self._table_ref.query(KeyConditionExpression=Key("_id").eq(key))
            return response["Items"][0]
            #
        except:
            return None

    def get_record(self, key):
        return json.loads(self._get_record(key)["json"])

    def write(self, df, upsert=False):
        """
        Write records and return only the ones that are written (new)
        """
        new_records = []
        for record in df.to_dict("records"):
            # https://github.com/boto/boto3/issues/665
            # record = json.loads(json.dumps(record), parse_float=Decimal)
            # either if we have a new record or in the case of UPSERT force the update
            existing = self._get_record(self._key_from_record(record))
            if not existing or upsert == True:
                try:
                    self._put_record(
                        {
                            "id": record["id"],
                            "brand": record["brand"],
                            "name": record["name"],
                            "status": record.get("status", "open"),
                            "created_at": record["created_at"],
                            "json": json.dumps(record),
                        }
                    )
                except:
                    # if we fail assume it is a payload thing for now and then retry
                    logger.warn(
                        f"Failed to write a record - try again later with {record['id']}"
                    )
                    #### if we have something leave it as is even though its stale
                    if not existing:
                        self._put_record(
                            {
                                "id": record["id"],
                                "brand": record["brand"],
                                "name": record["name"],
                                "status": record.get("status", "open"),
                                "created_at": record["created_at"],
                                "json": None,
                            }
                        )
                new_records.append(record)
        return pd.DataFrame(new_records)


class AirtableDynamoTable(DynamoTable):
    def __init__(self, table_id="airtable_cells", schema_id=None):
        """
            For the moment we create a specific Dynamo wrapper for the Airtable use case
            Later we should abstract features that useful more genreally to the super
            and possible deprecate this connector table

            We will create a small umber of managed tables e.g. 1 or 1 per base


            table = dynamodb.create_table(
            TableName='airtable_cells',
            KeySchema=[
                {
                        'AttributeName': '_id',
                        'KeyType': 'HASH'  # Partition key
                    },
                    # {
                    #     'AttributeName': 'timestamp',
                    #     'KeyType': 'RANGE'  # Sort key
                    # }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': k,
                    'AttributeType': 'S'
                }
                for k in [
                '_id'
                #, 'timestamp',
                ]
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        """

        self._table_id = table_id
        self._dynamodb = boto3.resource("dynamodb")
        self._table_ref = self._dynamodb.Table(self._table_id)

    def _key_from_record(self, value):
        # working with this convention *
        return f"{value['table_id']}/{value['record_id']}/{value['field_id']}"

    def lookup_record(self, record):
        key = self._key_from_record(record)
        return self._lookup_key(key)

    def _lookup_key(self, key):
        try:
            response = self._table_ref.query(KeyConditionExpression=Key("_id").eq(key))
            return response["Items"][0]["info"]
        except:
            return None

    def get_record(self, record_id):
        """
        Return a single record using the table index and filter by record id
        The data are stored as key-values with cells so pivot to a row
        Return the row as a dictionary if it exists or Null
        """
        return self.get_record_for_table(self._table_id, record_id)

    def _row_for_changed_record(self, changed_record):
        cols = ["table_id", "record_id"]  # "base_id",
        row = dict(zip(cols, [changed_record[c] for c in cols]))
        # try to update the record with everything we know about the row
        # this row is in the format row-level
        rec = (
            self.get_record_for_table(
                changed_record["table_id"], changed_record["record_id"]
            )
            or {}
        )
        row.update(rec)
        return row

    def get_record_for_table(self, table_id, record_id):
        response = self._table_ref.query(
            IndexName="table_id-index",
            FilterExpression=Attr("record_id").eq(record_id),
            KeyConditionExpression=Key("table_id").eq(table_id),
        )
        df = pd.DataFrame(i for i in response["Items"])
        df = df.pivot("record_id", "column_name", "cell_value")
        return None if len(df) == 0 else dict(df.iloc[0])

    def _is_date_greater(self, a, b):
        a = date_parse(str(a))
        b = date_parse(str(b))
        return a > b

    def _pack_change(self, old, new):
        """
        wrapper just to pack the change checking some types and names

        """

        # look for an old value or default to None
        keys = list(new.keys())

        for k in keys:
            default_value = (
                new[k] if k not in ["cell_value", "timestamp", "cursor"] else None
            )
            new[f"old_{k}"] = old[k] if k in old else default_value

        for k, v in new.items():
            if type(v) == Decimal:
                new[k] = float(v)
            # TODO:hack - coerce schema should do this but want to make sure pandas int-> flow does not cause problems
            try:
                if "cursor" in k:
                    new[k] = int(new[k])
            except:
                pass

        return new

    def upsert_newer(self, record):
        """
        check if their is no matching record or if value of the existing is different to proposed
         AND that the new timestamp is newer than the existing timestamp
        if check passes we insert otherwise we scope
        """
        key = self._key_from_record(record)

        # print("fetching item ", key, end="")
        existing_record = self._table_ref.get_item(Key={"_id": key})
        existing_record = (
            {} if "Item" not in existing_record else existing_record["Item"]
        )

        # print("done")

        newer = False
        if "timestamp" not in existing_record:
            newer = True
        elif self._is_date_greater(
            record["timestamp"], existing_record["timestamp"]
        ) and (str(existing_record["cell_value"]) != str(record["cell_value"])):
            newer = True

        if newer:
            self._put_record(record)
            return self._pack_change(existing_record, record)
        else:
            return None

    def upsert_changed(self, record):
        """
        check if their is no matching record or if value of the existing is different to proposed
        if check passes we insert otherwise we scope
        see also updated_newer
        """
        key = self._key_from_record(record)
        existing_record = self._table_ref.get_item(Key={"_id": key})

        if "Item" in existing_record:
            if str(existing_record["Item"]["cell_value"]) != str(record["cell_value"]):
                self._put_record(record)
        else:
            self._put_record(record)

    def _put_record(self, record):
        """
        wrapper around table write with some casting and other checks
        """
        record["_id"] = self._key_from_record(record)
        record["timestamp"] = record["timestamp"].isoformat()
        # TODO: think about proper managing types and cleaning etc
        # this is one where im curious about DDB or airtable not handling null books
        # when i set check box to false null is not handled properly
        record["cell_value"] = str(record["cell_value"])

        self._table_ref.put_item(Item=record)

    def _get_existing(self, changes):
        dynamodb = boto3.resource("dynamodb")
        records = (
            changes.sort_values("timestamp")
            .drop_duplicates(subset=["record_id", "table_id", "field_id"], keep="last")
            .to_dict("records")
        )

        items = {}
        # dynamo allows 100 keys to be requested at a time
        for c in chunk(records, 100):
            batch_keys = {
                "airtable_cells": {
                    "Keys": [{"_id": self._key_from_record(r)} for r in c]
                },
            }
            response = dynamodb.batch_get_item(RequestItems=batch_keys)
            existing = response["Responses"]["airtable_cells"]

            for i in existing:
                items[i["_id"]] = i

        return items

    def insert_new_batch(self, changes, record_info_delegate=None, plan=False):
        if len(changes) == 0:
            return changes

        existing = self._get_existing(changes)
        if len(existing) > 0:
            logger.debug(
                f"info: We fetched {len(existing)} existing keys e.g. {list(existing.keys())[0]} when checking against the incoming batch of size {len(changes)}"
            )
        else:
            logger.debug(
                "info: there are no existing records matching these keys - we should be able to insert all of them as new"
            )
        # print(f"check exists : {existing}/ {len(changes)}")
        # new = changes[~changes.apply(self._key_from_record, axis=1).isin(existing)]
        new_additions = []
        with self._table_ref.batch_writer(overwrite_by_pkeys=["_id"]) as batch:
            for i, record in enumerate(changes.to_dict("records")):
                if i == 0:
                    print(f"info: sample record for insertion in batch {record}")

                record["_id"] = key = self._key_from_record(record)
                record["cell_value"] = str(record["cell_value"])
                # record info can be added to cell changes such as keys, flows, nodes and ONE ids
                record_info = {}
                existing_item = {}

                if key in existing:
                    existing_item = existing[key]
                    # these are used for comparison
                    value, timestamp = (
                        existing_item["cell_value"],
                        existing_item["timestamp"],
                    )

                    existing_item["cell_value"] = str(existing_item["cell_value"])
                    # if we have a newer different value
                    newer = record["cell_value"] != value and self._is_date_greater(
                        record["timestamp"], timestamp
                    )

                    if not newer:
                        continue

                record["timestamp"] = record["timestamp"].isoformat()
                for k, v in record.items():
                    if isinstance(v, float):
                        record[k] = Decimal(str(v))

                if not plan:
                    batch.put_item(Item=record)

                # create the change record
                record = self._pack_change(existing_item, dict(record))
                # add additional info while we have it from the row
                record["info"] = json.dumps(record_info)
                new_additions.append(record)

        return pd.DataFrame(new_additions)

    def _write_records(self, records, check_exists=False, batch=False):
        """
        Write a single dict or a collection of dicts to DynamoDB
        We generate the airtable field key based on table/record/field
        The time is used as a sort key - dynamo wants a string
        - (TODO:) I assume it is a proper datetime but we *could* try to parse a string too
        """

        # handle dict or collection of dicts
        if isinstance(records, dict):
            records = [records]

        # note that we are overwriting in the same set with duplicates which may not be what we want
        # if we disable this top level we can swtich to single item upserts (e.g. as an env setting)
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
        if batch:
            with self._table_ref.batch_writer(
                overwrite_by_pkeys=["_id", "timestamp"]
            ) as batch:
                for record in records:
                    record["_id"] = self._key_from_record(record)
                    record["timestamp"] = record["timestamp"].isoformat()
                    batch.put_item(Item=record)
        else:
            for record in records:
                # self._put_record(record)
                result = self.upsert_newer(record)
                if result is not None:
                    yield result
