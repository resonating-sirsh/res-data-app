import pandas as pd


class DatabaseConnectorTable(object):
    def _init_(self, schema):
        pass

    def iterator(self, fields=None, filters=None, view=None):
        print("IN PARENT")

    def _changes(self, cursor, timestamp_watermark=None):
        pass

    def to_dataframe(
        self,
        fields=None,
        filters=None,
        view=None,
        **options,
    ):
        return pd.DataFrame(
            [
                row
                for row in self.iterator(
                    fields=fields, filters=filters, view=view, **options
                )
            ]
        )

    def update_record(self, record, **options):
        pass

    def update(self, dataframe, **options):
        for row in dataframe.to_dict("records"):
            self.update_record(row, **options)

    def _update_record_newer(self, record, *options):
        pass


class DatabaseConnectorSchema(object):
    def _init_(self, connector):
        pass

    def _get_client(self):
        pass

    def _table_metadata(self, table_name):
        pass

    @property
    def metadata(self):
        pass

    def _get_meta_dataframe(self, **options):
        pass

    @property
    def name(self):
        pass

    # getter for all tables


class DatabaseConnector(object):
    def __init__(self, **options):
        pass

    def _get_schema(self):
        pass

    def _get_client(self):
        pass

    # getter for all schemas/catalogis or whatever is useful


# a connector can implement a schema manager
# S3, Dgraph etc. can all implement this interface and provide metadata for schema
class SchemaManager(object):
    def __init__(self, **options):
        pass

    def coerce(self, df):
        # load metadata
        # rename and white list fields
        # coerce types reliably
        pass

    def to_schema(self, schema_type):
        # avro, druid, dgraph, etc.
        pass


# usage patters
#
#   Airtable['base_id']['table_id'].changes(cursor) #this calls out to webhooks where supported
#   Airtable['base_id']['table_id'].to_dateframe(fields, filters)
#   Airtable['base_id].table_metadata
#
#   Mongo['database']['collection'].to_dateframe(fields, filters)
#
#   Dynamo['database']['table_id'].to_dataframe(fields, flters) #this requires index management
#
#
#
#
#
