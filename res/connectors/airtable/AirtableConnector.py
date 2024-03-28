from res.utils import dataframes, dates
from .. import DatabaseConnector, DatabaseConnectorSchema, DatabaseConnectorTable
from . import AirtableWebhooks
from .readers import stream_cell_changes, merge_table_sets
from . import formatting
from airtable import Airtable as AirtableWrapper
from . import logger, safe_http, get_airtable_request_header
from . import (
    AIRTABLE_LATEST_S3_BUCKET,
    AIRTABLE_VERSIONED_S3_BUCKET,
    formatting as airtable_formatting,
)
from . import S3Connector
from .AirtableWebhooks import AirtableWebhooks
import json
import pandas as pd
import os
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from res.utils.secrets import secrets_client
import res
from urllib.parse import quote
from time import sleep
import requests
from res.connectors.airtable.AirtableMetadataCache import AirtableMetadataCache

# we use this for additional metadata
# https://airtable.com/tblQiFyKtjnxdG1Rw
FIELDS_TABLE = "tblQiFyKtjnxdG1Rw"
FIELDS_BASE = "appc7VJXjoGsSOjmw"
FIELDS_TABLE_COLUMNS = [
    "Name",
    "Type",
    "Table ID",
    "Base ID",
    "res.Network Field Type",
    "add_webhook",
    "webhook_default_profile",
    # could add the provider info but we don't really use it tet
    "ID",
    "Config",
]
PARTITION_KEY_SORT_COLUMN = "__timestamp__"

# this is mapping to the airtable cdc schema
CDC_FROM_CHANGES_MAPPING = {
    "table_id": "table_id",
    "field_id": "field_id",
    "record_id": "record_id",
    "timestamp": "timestamp",
    "cell_value": "value",
    "old_cell_value": "old_value",
    "old_timestamp": "old_timestamp",
    "meta_type": "meta_type",
    "column_name": "field_name",
    # "duration_seconds": "duration_seconds",
}

# slop
AIRTABLE_TIMEOUT = None  # None == no timeout == same as what pyairtable does.

RES_ENV = os.environ.get("RES_ENV")
AIRTABLE_ONE_PLATFORM_BASE = (
    "ONE Platform - development" if RES_ENV != "production" else "ONE Platform"
)


def get_schema_by_name(name):
    s3 = res.connectors.load("s3")
    return s3.try_load_meta_schema(name)


class AirtableKeyCache:
    def __init__(self, table_id, cold_storage_provider=None):
        self._cache = res.connectors.load("redis")["AIRTABLE_CACHE"]["KEYS"]
        self._cold_storage = cold_storage_provider
        self._table_id = table_id

    def _make_akey(self, key):
        return f"{self._table_id}-{key}"

    def __getitem__(self, key):
        key = self._make_akey(key)
        # print('getting',key)
        value = self._cache[key]
        if not value and self._cold_storage:
            value = self._cold_storage[key]
            if value:
                self._cache[key] = value
        return value

    def __setitem__(self, key, value):
        key = self._make_akey(key)
        # print('setting',key,value)
        self._cache[key] = value
        if self._cold_storage:
            self._cold_storage[key] = value

    def delete(self, key):
        key = self._make_akey(key)
        # print('removing', key)
        self._cache.delete(key)
        if self._cold_storage:
            del self._cold_storage[key]


class AirtableConnector(DatabaseConnector):
    """
    Top level wrapper around airtable API providing access to bases and tables

    """

    API_URI = "https://api.airtable.com/v0"
    # fetch for the static use cases

    RENAME_API_WEBHOOK_DATAFIELDS = {}
    RENAME_TABLE_GETTER_DATAFIELDS = {}

    def __init__(self, profile=None):
        self._profile = profile or "default"
        # when constructed ask for the secret
        if "AIRTABLE_API_KEY" not in os.environ:
            secrets_client.get_secret("AIRTABLE_API_KEY")

    def __getitem__(self, base_id_or_name):
        if base_id_or_name[:3] != "app":
            bases = AirtableConnector.get_base_names()
            match = bases[
                (bases["name"] == base_id_or_name)
                | (bases["clean_name"] == base_id_or_name)
            ]
            if len(match) > 0:
                base_id_or_name = match.iloc[0]["id"]
        return AirtableBase(base_id_or_name, self)

    def make_key_lookup_predicate(keys, name):
        if not isinstance(keys, list):
            keys = [keys]

        pred = ",".join([f"{{{name}}}='{key}'" for key in keys])
        pred = f"OR({pred})"
        return pred

    def download_attachments(self, uri):
        data = safe_http.request_get(
            uri, headers=get_airtable_request_header(), timeout=AIRTABLE_TIMEOUT
        )
        return data

    # could be a self method sync(self,records)
    def airtable_sync(
        table, records, table_options=None, retry_waits=[50, 50, 50, 50], plan=False
    ):
        """
        bulk queue sync logic for table : base_id/table_id
        records are from our own store
        we return the state that we want to sync - locally we can take things with pull or sync and update our store which will be correct seee below
        remotely we only update airtable if plan is False

        - we make attachments to s3 paths if relevant
        - we fetch row hash to know when we need to update things
        - if the status or tags are updated in airtable, we use as source of truth and replace ours
        """

        def make_attachments():
            return []

        base_id, table_id = table.split("/")
        key = "key"
        # for test we use these but they are controlled by table options these MUST be either things that dont change or are owned by airtable. the sys fields are computed anyway.
        remote_fields = [key, "Status", "Tags", "Notes", "sys.primitive_hash"]
        airtable = res.connectors.load("airtable")
        tab = airtable[base_id][table_id]

        tab.check_schema(table_options, sync_changes=True)

        remote_data = tab.to_dataframe(fields=remote_fields)

        # run some watermark checks etc..

        def with_retries(data, action):
            response = {}
            for retry, wait in enumerate(retry_waits):
                try:
                    response["retry"] = retry
                    response["errors"] = None
                    response["data"] = action(data)
                    break
                except Exception as ex:
                    response["errors"] = ex
                    res.utils.logger.debug(
                        f"Sleeping after fail {res.utils.ex_repr(ex)} - {wait} seconds"
                    )
                    sleep(wait)
            return response

        status = []

        for record in records:
            remote_record = remote_data.get(key)
            date = dates.utc_now()

            action = "no_action"  # firstly must have changes including status dates for purge.
            if not remote_record:
                action = "create"
            elif record.get("sys.purge_queue_ts"):
                action = "delete"
            elif remote_record["sys.primitive_hash"] != record["sys.primitive_hash"]:
                action = "sync"
                # we can determinte if its push or pull but dont think it matters yet
                # its pull unless the remotely managed fields have changed

            if action == "delete":
                if not plan:
                    response = with_retries(record, tab.delete_record)
            else:
                if action in ["pull", "sync"]:
                    # the remote owned fields are pull down - because this is pull or sync we can later send this to dgraph or we can do it here by creating and applying the mutation
                    for key in remote_fields:
                        record[key] = remote_record[key]

                if not plan and action in ["push", "sync"]:
                    tab.update_attachment_links(record, table_options)
                    response = with_retries(record, tab.update_record)

            response_date = dates.utc_now()

            status.append(
                {
                    "data": record,
                    "request_date": date,
                    "response_date": response_date,
                    "action": action.upper(),
                    "errors": response["errors"],
                    "retries": response["retry"],
                }
            )

        df = pd.DataFrame(status)
        df["table"] = table
        df["status"] = "OK"
        df["response_time_seconds"] = (
            df["response_date"] - df["request_date"]
        ).total_seconds()
        # leave the ndates as strings for serialize
        df["request_date"] = df["request_data"].map(
            lambda d: d.replace(None).isoformat()
        )
        df["response_date"] = df["response_date"].map(
            lambda d: d.replace(None).isoformat()
        )
        df.loc[df["errors"].notnull(), "status"] - "FAIL"

        return df

    def get_table(self, path):
        """
        supply BASE/TABLE
        """
        return self[path.split("/")[0]][path.split("/")[1]]

    def get_view(self, path):
        """
        supply BASE/TABLE/VIEW
        """
        return self[path.split("/")[0]][path.split("/")[1]]

    def get_table_data(self, path, fields=None, last_update_since_hours=None):
        """
        supply BASE/TABLE
        """
        return self[path.split("/")[0]][path.split("/")[1]].to_dataframe(
            fields=fields, last_update_since_hours=last_update_since_hours
        )

    def get_view_data(self, path, fields=None, last_update_since_hours=None):
        """
        supply BASE/TABLE/VIEW
        """
        return self[path.split("/")[0]][path.split("/")[1]].to_dataframe(
            view=path.split("/")[2],
            fields=fields,
            last_update_since_hours=last_update_since_hours,
        )

    @staticmethod
    def get_base_names(
        try_from_cache=False, force_reload_from_airtable_metadatacache=False
    ):
        """
        lookups between ids and names of tables
        """
        if try_from_cache:
            try:
                cache = AirtableMetadataCache.get_instance()
                ds_all_base_names = cache.get_all_base_names(
                    force_reload_from_airtable=force_reload_from_airtable_metadatacache
                )
                return ds_all_base_names
            except Exception as ex:
                res.utils.logger.warn(
                    f"AirtableMetadataCache cache not accessible ex:{ex}"
                )

        res.utils.logger.debug(f"Fetching base metadata - consider using cache")

        url = "https://api.airtable.com/v0/meta/bases"
        data = pd.DataFrame()
        offset = None
        while True:
            json_data = safe_http.request_get(
                f"{url}?offset={offset}" if offset is not None else url,
                headers=get_airtable_request_header(),
                timeout=AIRTABLE_TIMEOUT,
            ).json()
            data = pd.concat([data, pd.DataFrame(json_data["bases"])])
            offset = json_data.get("offset")
            if offset is None:
                break
        data["clean_name"] = data["name"].map(formatting.strip_emoji)

        return data

    def _get_base_tables_metadata_full(self, base_id):
        # sirsh, this is only ever called from a place that is already protected by cache, so does not need to be refactored - this is the 'fallback to airtable itself' code
        metadata_url = f"{AirtableConnector.API_URI}/meta/bases/{base_id}/tables"
        json_data = safe_http.request_get(
            metadata_url,
            headers=get_airtable_request_header(),
            timeout=AIRTABLE_TIMEOUT,
        ).json()
        df = pd.DataFrame([r for r in json_data["tables"]])
        df["clean_name"] = df["name"].map(formatting.strip_emoji)
        return df

    def _get_base_tables_metadata(
        self, base_id, try_from_cache=True, try_from_airtablemetadatacache_first=True
    ):
        if try_from_airtablemetadatacache_first:
            try:
                cache = AirtableMetadataCache.get_instance()
                df_tables_in_base = cache.get_tables_in_base(
                    base_id, force_reload_from_airtable=not try_from_cache
                )
                return df_tables_in_base
            except Exception as ex:
                res.utils.logger.warn(
                    f"AirtableMetadataCache cache not accessible {ex}"
                )

        res.utils.logger.debug(
            f"fetching metadata from airtable for base {base_id} - consider using cache"
        )
        metadata_url = f"{AirtableConnector.API_URI}/meta/bases/{base_id}/tables"
        json_data = safe_http.request_get(
            metadata_url,
            headers=get_airtable_request_header(),
            timeout=AIRTABLE_TIMEOUT,
        ).json()
        df = pd.DataFrame([r for r in json_data["tables"]])
        df = df[["id", "name"]]
        df["clean_name"] = df["name"].map(formatting.strip_emoji)

        return df

    def update_queue(
        self,
        schema,
        base_id=None,
        res_schema_mapping=None,
        res_type_name=None,
        **kwargs,
    ):
        """
        update the airtable queue: this is experimental may move
        """

        from . import AirtableWebhookSpec

        # default base id
        base_id = base_id or "appaaqq2FgooqpAnQ"

        if isinstance(schema, str):
            schema = json.loads(schema)

        api_root = f"{AirtableConnector.API_URI}/meta/bases/{base_id}/tables"
        name = schema["name"]
        table = self[base_id].get_table(name)
        if table is None:
            res.utils.logger.info(f"creating a new table {name}")
            response = safe_http.request_post(
                api_root,
                schema,
                headers=get_airtable_request_header(),
                timeout=AIRTABLE_TIMEOUT,
            )
            table_id = response.json().get("id")
        else:
            res.utils.logger.info(
                "AS THIS IS TEST CODE WE WILL NOT ALTER EXISTING TABLES WHEN CREATING QUEUES"
            )
            table_id = table.iloc[0]["table_id"]
            # do a diff / create and update fields

        # do some validation and paths - assume success create for now

        field_names = [f["name"] for f in schema["fields"]]
        # update the webhook now

        res.utils.logger.info(
            f"creating webhook for {base_id}/{table_id} ({res_type_name}) fields {field_names}"
        )
        spec = AirtableWebhookSpec.get_spec_for_field_names(
            base_id,
            table_id,
            field_names,
            table_name=name,
            # this is a special mapping to our schema convention
            res_schema_mapping=res_schema_mapping,
            res_type_name=res_type_name,
        )

        spec.save_config()
        wid, remote_spec = spec.push()

        # when we get updates CHANGES from the webhook on TODO > we write to kafka for configured topics
        return remote_spec

    @staticmethod
    def airtable_schema_from_df(df, name, multi_select_fields=None):
        fields = []

        for key, value in df.dtypes.items():
            d = {"name": key, "type": "singleLineText"}

            if key in multi_select_fields:
                d["type"] = "multipleSelects"
                d["options"] = {"choices": [{"name": "*"}]}

            if "int" in str(value):
                d["type"] = "number"
                d["options"] = {"precision": 0}
            if "float" in str(value):
                d["type"] = "number"
                d["options"] = {"precision": 2}
            if "bool" in (str(value)):
                d["type"] = "checkbox"
                d["options"] = {"icon": "check", "color": "greenBright"}

            if "datetime" in (str(value)):
                d["type"] = "dateTime"
                d["options"] = {
                    "dateFormat": {"name": "iso"},
                    "timeFormat": {"name": "24hour"},
                    "timeZone": "utc",
                }

            fields.append(d)

        return {
            # for testing we are going to make the name match what we do for the kafka topic
            "name": name,
            "fields": fields,
        }

    @staticmethod
    def get_airtable_table_for_schema_by_name(
        schema_name,
        filters=None,
        view=None,
        provider=None,
        last_update_since_hours=None,
        remmove_pseudo_lists=True,
    ):
        """
        the schema can be loaded by name from a provider
        can migrate between providers e.g.

            import res, yaml
            dgraph = res.connectors.load('dgraph')
            s3 = res.connectors.load('s3')
            for s in ['styles', 'make.material_properties']:
                v = dgraph.try_load_meta_schema(s)
                v = yaml.safe_dump(v)
                s = s.replace('.','-)
                s3.write(f"s3://res-data-platform/schema/{s}.yaml",v )

        """
        from res.connectors import load as load_connector

        s3 = load_connector("s3")
        # change the default to s3 but need to migrate schema
        provider = provider or s3

        schema = provider.try_load_meta_schema(schema_name)
        return AirtableConnector.get_airtable_table_for_schema(
            schema,
            filters,
            view=view,
            last_update_since_hours=last_update_since_hours,
            remmove_pseudo_lists=remmove_pseudo_lists,
        )

    @staticmethod
    def get_airtable_table_for_schema(
        schema,
        filters=None,
        adding_unmapped_schema_fields=True,
        remmove_pseudo_lists=True,
        load_history=False,
        table=None,
        view=None,
        last_update_since_hours=None,
    ):
        """
        convert something in airtable schema to our schema
        if not table passed in we load it from the schema context so this can be used to load/transform source data or transform passed in data
        """
        if not table:
            at = AirtableConnector()
            table = at[schema["airtable_base_id"]][schema["airtable_table_id"]]

        field_metadata = pd.DataFrame(schema["fields"])[
            ["airtable_field_name", "name", "key"]
        ]
        field_metadata["name"] = field_metadata["name"].fillna(field_metadata["key"])
        # only the fields that are define for airtable should be read
        fields = dict(field_metadata[["airtable_field_name", "name"]].dropna().values)
        data = table.to_dataframe(
            fields=list(fields.keys()),
            filters=filters,
            view=view,
            last_update_since_hours=last_update_since_hours,
        ).rename(columns=fields)

        if adding_unmapped_schema_fields:
            for f in schema["fields"]:
                field_name = f.get("name", f.get("key"))
                if field_name and field_name not in data.columns:
                    data[field_name] = None

        if remmove_pseudo_lists:
            data = data.applymap(airtable_formatting.remove_pseudo_lists)

        return data

    @staticmethod
    def map_to_res_schema(df, schema):
        """
        Maps to res schema in a strict sense i.e. drop fields that are not on the schema
        We could implement a weaker one in future where it renames what it can and leaves the rest - this is just using the the normal dataframe rename
        """
        field_metadata = pd.DataFrame(schema["fields"])[["airtable_field_name", "name"]]

        # only the fields that are define for airtable should be read
        fields = dict(field_metadata[["airtable_field_name", "name"]].dropna().values)

        return dataframes.rename_and_whitelist(df, fields)

    @staticmethod
    def make_change_pivot(data, schema=None):
        """
        Utility method to take cell changes and pivot them into latest rows per record id
        """
        data = data.sort_values("timestamp").drop_duplicates(
            subset=["record_id", "column_name"], keep="last"
        )
        ts = data[["record_id", "timestamp"]].groupby("record_id").max()
        data = data.pivot("record_id", "column_name", "cell_value").join(ts)

        if schema:
            data = AirtableConnector.map_to_res_schema(data, schema)

        # this is how we keep the record id
        return data.reset_index()

    @staticmethod
    def cell_status_changes_with_attributes(
        changes, full_stream, track_columns, attributes=["Name"]
    ):
        """
        By combining the tracking provider which is dynamo and using standard column names
        We track each cell change and merge on the row attributes for that record
        We return an event which shows the duration as a pair of timestamps with the attributes and new and old cell value
        """
        if not isinstance(track_columns, list):
            track_columns = [track_columns]
        if not isinstance(attributes, list):
            attributes = [attributes]
        cols = [
            "record_id",
            "column_name",
            "cell_value",
            "timestamp",
            "old_timestamp",
            "_id",
            "cursor",
            "old_cell_value",
        ]
        piv = AirtableConnector.make_change_pivot(full_stream)[attributes].reset_index()
        all_data = []
        for c in track_columns:
            # the rename is a partial pivot so it would be wring if there was an overlap with attributes and track columns
            data = changes[changes["column_name"] == c][cols].rename(
                columns={"cell_value": c}
            )
            data = pd.merge(data, piv, on="record_id")
            all_data.append(data)
        data = pd.concat(all_data).reset_index().drop("index", 1)
        for c in ["timestamp", "old_timestamp"]:
            if not is_datetime(data[c]):
                data[c] = pd.to_datetime(data[c])

        return data.sort_values("timestamp").drop_duplicates(
            subset=attributes + ["old_timestamp", "column_name"]
        )

    @staticmethod
    def cdc(changes, schema, change_provider=None, plan=False):
        """
        Compute the changed records in our schema and compute the change rows with before-after-hour segment schema
        The changed records can be saved to dgraph or any other database and the the cdc events can be sent to kafka
        The airtable webhook client can manage this relay

        (note) The hourly segments can be expanded and sent to druid for laser cutter and other resources that match to resource types
        The resSchema replaces the way we managed contracts beore as now every airtable belongs to a res schema and we know if things are resources or ones or whatever that are changing kanban states
        """
        from res.connectors.dynamo import AirtableDynamoTable

        change_provider = change_provider or AirtableDynamoTable()
        field_metadata = pd.DataFrame(schema["fields"])[
            ["airtable_field_name", "name", "key", "is_key"]
        ]

        key_col = field_metadata[field_metadata["is_key"] == True].iloc[0][
            "airtable_field_name"
        ]
        base_id = schema.get("airtable_base_id")

        def filter_changes_by_schema(data):
            """
            This function provides opportunity to see how res-schema controls what we watch and track
            For now we are just managing any fields that are in the schema - this means CDC depends entirely on an up-to-date res schema
            """

            airtable_fields = list(
                field_metadata["airtable_field_name"].dropna().values
            )
            logger.debug(
                f"Filtering changes to watched fields {airtable_fields} with key {key_col}"
            )
            # filter out the fields we are watching in our schema
            return (
                data[data["column_name"].isin(airtable_fields)]
                if len(data) > 0
                else data
            )

        # get cells that are actually changed while inserting the new ones - if plan==true, we only probe changes which is good for testing
        confirmed_changes = change_provider.insert_new_batch(
            filter_changes_by_schema(changes), plan=plan
        )

        if len(confirmed_changes) == 0:
            return None, None

        try:
            # TODO:
            res.utils.logger.debug(
                f"Removing pseudo lists - check schema if this makes sense"
            )
            confirmed_changes["cell_value"] = confirmed_changes["cell_value"].map(
                airtable_formatting.remove_pseudo_lists
            )
            confirmed_changes["old_value"] = confirmed_changes["old_value"].map(
                airtable_formatting.remove_pseudo_lists
            )

        except:
            pass

        # create as many "status" rows as there are changed records - these are Upserted into status tables matching **our** schema
        changed_record_ids = confirmed_changes["record_id"].unique()
        # because we pass the schema, row_data should be in the res schema
        row_data = AirtableConnector.make_change_pivot(
            changes[changes["record_id"].isin(changed_record_ids)], schema=schema
        )

        # primary keys by record using whatever the primary key configured in the schema
        keys = dict(
            changes[changes["column_name"] == key_col][
                ["record_id", "cell_value"]
            ].values
        )

        cdc = dataframes.rename_and_whitelist(
            confirmed_changes, CDC_FROM_CHANGES_MAPPING
        )
        cdc["base_id"] = base_id
        cdc["primary_key"] = cdc["record_id"].map(lambda x: keys[x])
        # the res meta type is store for doing asset and resource groupings/routings
        cdc["meta_type"] = schema.get("key")
        # for new records create defaults
        cdc["old_timestamp"] = cdc["old_timestamp"].fillna(cdc["timestamp"])
        cdc["old_value"] = cdc["old_value"].fillna("NONE")

        # return the tuple that is the changed rows in our schema and the CDC events
        # the first can be saved in a transactional database like postgres or dgraph and the later can be sent onward to kafka
        return row_data, cdc


class AirtableBase(DatabaseConnectorSchema):
    """
    This is a wrapper around Airtable bases which are like 'schema'
    It simply wraps access to tables
    """

    def __init__(self, base_id, connector):
        self._base_id = base_id
        self._set_base_name()
        self._connector = connector

    def _get_client(self):
        at = AirtableWrapper(self._base_id, os.environ["AIRTABLE_API_KEY"])
        return at

    def webhook_by_id(self, webhook_id, check_owns):
        table_id = AirtableWebhooks._table_id_from_webhook_id(webhook_id)
        return AirtableWebhooks(
            profile=self._connector._profile,
            table_id=table_id,
            base_id=self._base_id,
            check_owns=check_owns,
        )

    def _set_base_name(self):
        try:
            self._base_name = (
                AirtableConnector.get_base_names()
                .set_index("id")
                .loc[self.base_id]["clean_name"]
            )
        except:
            # full refresh
            self._base_name = (
                AirtableConnector.get_base_names(
                    try_from_cache=True, force_reload_from_airtable_metadatacache=True
                )
                .set_index("id")
                .loc[self.base_id]["clean_name"]
            )

    def get_table_id_from_name(self, table_name):
        return dict(self.get_base_tables()[["name", "id"]].values).get(table_name)

    def reload_cache(self, table_id=None, use_airtablemetadatacache_first=True):
        if table_id:
            return self.get_table_fields(
                table_id,
                try_from_cache=False,
                use_airtablemetadatacache_first=use_airtablemetadatacache_first,
            )
        return AirtableConnector()._get_base_tables_metadata(
            base_id=self.base_id,
            try_from_cache=False,
            try_from_airtablemetadatacache_first=use_airtablemetadatacache_first,
        )

    def get_table_name(self, table_id, clean_name=True):
        try:
            # try to read from the AirtableMetadataCache
            cache = AirtableMetadataCache.get_instance()
            tablename = cache.get_table_name(
                self.base_id, table_id_or_name=table_id, clean_name=clean_name
            )
            return tablename
        except Exception as ex:
            res.utils.logger.warn(
                f"AirtableMetadataCache could not get_table_name, ex: {ex}"
            )

        # AirtableMetadataCache threw an excption, fall back to existing airtable call out
        data = self.resolve_table_record_from_id_or_name(table_id)
        return data["name"] if not clean_name else data["clean_name"]

    def resolve_table_record_from_id_or_name(self, table_id):
        # Only place this is called from is already protected by the AirtableMetadataCache so no need to update this func to use AirtableMetadataCache
        # as this func only being called if call to AirtableMetadataCache failed. So leave as-is in directly contacting airtable for metadata"""
        d = AirtableConnector()._get_base_tables_metadata_full(self.base_id)
        if table_id[:3] == "tbl":
            d = d[d["id"] == table_id]
        else:
            d = d[d["name"] == table_id]
            if len(d) == 0:
                res.utils.logger.debug(
                    f"missing {table_id} - reloading cache just in case"
                )
                d = self.reload_cache()
                d = d[d["name"] == table_id]

        if len(d) == 0:
            raise Exception(
                f"Could not find table {table_id} in base {self._base_id} even after reloading the cache."
            )

        return d.iloc[0]

    def get_table_fields(
        self, table_id, try_from_cache=True, use_airtablemetadatacache_first=True
    ):
        if use_airtablemetadatacache_first:
            try:
                cache = AirtableMetadataCache.get_instance()
                ds_fields_in_table = cache.get_table_fields(
                    base_id=self.base_id,
                    table_id=table_id,
                    force_reload_from_airtable=not try_from_cache,
                )
                return ds_fields_in_table
            except Exception as ex:
                res.utils.logger.debug(
                    f"Airtable cache not accessible {ex} will load fresh for table {table_id}"
                )

        res.utils.logger.debug(
            f"fetching table metadata for table {table_id} - consider using cache"
        )
        d = self.resolve_table_record_from_id_or_name(table_id)

        fields = pd.DataFrame(d["fields"])
        fields["is_key"] = fields["id"].map(lambda x: x == d["primaryFieldId"])
        fields = fields.drop("options", 1)

        # removed cache redis writing
        return fields

    @property
    def connector(self):
        return self._connector

    @property
    def base_id(self):
        return self._base_id

    @property
    def table_ids(self):
        return self._table_ids

    def get_base_tables(self, use_cache=True):
        data = AirtableConnector()._get_base_tables_metadata(
            self._base_id, try_from_cache=use_cache
        )
        data["clean_name"] = data["name"].map(formatting.strip_emoji)

        return data

    def __getitem__(self, table_id_or_name):
        if table_id_or_name[:3] != "tbl":
            tables = self.get_base_tables()
            match = tables[
                (tables["name"] == table_id_or_name)
                | (tables["clean_name"] == table_id_or_name)
            ]
            if len(match) > 0:
                table_id_or_name = match.iloc[0]["id"]
        return Airtable(table_id_or_name, self)

    def create_new_table(
        self,
        table_name,
        key_name,
        default_fields=["Status", "Tags", "Notes"],
        description="res-data created this table",
        plan=False,
    ):
        """
        Create a table if it does not exist
        https://airtable.com/api/enterprise#enterpriseBaseTableCreateTable
        At least one field must be specified. The first field in the fields array will be used as the table's primary field and must be a supported primary field type. Fields must have case-insensitive unique names within the table.

        """

        # dont use the cache because we need to be aggressive here in checking state
        existing_tables = list(self.get_base_tables(use_cache=False)["name"])

        def type_for_field(f):
            """
            conventional create types
            """
            d = {
                "name": f,
                "description": "Default field added by platform",
                "type": "singleLineText",
            }

            if f == "Tags":
                d["type"] = "multipleSelects"
                d[
                    "description"
                ] = "Default field added by platform for adding multiple tags usually to flag issues"
                d["options"] = {"choices": [{"name": "*"}]}

            if f == "Status":
                d["type"] = "singleSelect"
                d[
                    "description"
                ] = "Default field added by platform for adding multiple tags usually to flag issues"
                d["options"] = {
                    "choices": [
                        {"name": "TODO"},
                        {"name": "DONE", "color": "greenDark1"},
                        {"name": "FAILED", "color": "redDark1"},
                        {"name": "CANCELLED", "color": "redBright"},
                        {"name": "PROGRESSING", "color": "blueLight2"},
                    ]
                }

            if f == "Notes":
                d["type"] = "multilineText"
                d[
                    "description"
                ] = "Default field added by platform for adding longer form notes"

            return d

        if table_name not in existing_tables:
            uri = f"{AirtableConnector.API_URI}/meta/bases/{self.base_id}/tables"
            payload = {
                "name": table_name,
                # "description": description,
                "fields": [
                    {
                        "name": key_name,
                        # "description": "The key field",
                        "type": "singleLineText",
                    },
                ]
                + [type_for_field(f) for f in default_fields],
            }

            if plan:
                return payload

            r = safe_http.request_post(
                uri,
                json=payload,
                headers=get_airtable_request_header(),
                timeout=AIRTABLE_TIMEOUT,
            ).json()

            self.reload_cache()
            # and update the table meta data too
            self.reload_cache(r["id"])

            return r
        else:
            res.utils.logger.info(
                f"Table [{table_name}] present on base {self.base_id}"
            )

            return {"id": self.get_table_id_from_name(table_name)}

    def get_table(self, table):
        """
        get_table('tblJ3CzpX3sAuYicr')
        get table metadata so we can make decisions about upserts
        """
        try:
            cache = AirtableMetadataCache.get_instance()
            table_id = cache.resolve_table_id_from_name_or_id(self._base_id, table)
            df_table_fields = cache.get_table_fields(self._base_id, table_id)
            df_table_fields["table_id"] = table_id
            return df_table_fields

        except Exception as ex:
            res.utils.logger.warn(
                f"AirtableMetadataCache was unable to retreieve table metadata for: {table}. exception: {ex}"
            )

        def with_table_id(f, tid):
            f["table_id"] = tid
            return f

        api_root = f"{AirtableConnector.API_URI}/meta/bases/{self._base_id}/tables"
        try:
            response = safe_http.request_get(
                api_root,
                headers=get_airtable_request_header(),
                timeout=AIRTABLE_TIMEOUT,
            )
            data = response.json()
            for d in data["tables"]:
                if d["name"] == table or d["id"] == table:
                    return pd.DataFrame(
                        [with_table_id(f, d["id"]) for f in d["fields"]]
                    )
        except Exception as ex:
            print(ex)
        # to distinguish between errors

    # manage metadata including extended metadata from our fields table "custom providers"


class Airtable(DatabaseConnectorTable):
    """
    This is the main class for accessing airtable
    Connectors and bases are wrappers to index into the correct table
    Most operations are confined to tables

    this class wraps both the webhooks and the standard airtable api
    Webhooks are used to provide changes as dataframes
    Tables can be loaded as dataframes specifing fields and filters

    This class also provides access to our custom Resonance metadata for Airtable tables and fields

    The webhook code and metadata used to manage sessions etc are split into
    - AirtableWebhooks
    - AirtableMetadata
    """

    def __init__(self, table_id, base):
        self._reinit(table_id, base)

    def _reinit(self, table_id, base):
        self._table_id = table_id
        self._friendly_name = None
        self._base = base
        self._base_id = self._base._base_id
        self._basename = self._base.name
        self._connector = self._base.connector
        self._field_metadata = self._base.get_table_fields(
            self._table_id
        )  # dict(self._base._metadata.loc[self._table_id])
        try:
            self._friendly_name = self._base.get_table_name(self._table_id)
        except:
            self._friendly_name = "DONT KNOW TRY RELOAD CACHE"
        # table metadata, what fields are flow, node, ONE, key {record_info}
        # given a collection of cells, create info objects for each record
        self.record_info_delegate = None
        self._key_field = self._field_metadata[self._field_metadata["is_key"]].iloc[0][
            "name"
        ]
        self._key_cache = AirtableKeyCache(self._table_id)

    def __getitem__(self, key):
        """
        determines the primary key field and looks up the item by key
        sorts by timestamp incase of duplicates and tales last
        """
        return dict(
            self.to_dataframe(filters=self._key_lookup_predicate(key))
            .sort_values("__timestamp__")
            .iloc[-1]
        )

    @property
    def key_cache(self):
        return self._key_cache

    def reload_cache(self):
        self._base.reload_cache(table_id=self.table_id)
        self._reinit(self.table_id, self._base)
        return self

    def update_field_schema(self, payload):
        """
        POST https://api.airtable.com/v0/meta/bases/BaseId/tables/TableId/fields
        """

        uri = f"{AirtableConnector.API_URI}/meta/bases/{self.base_id}/tables/{self._table_id}/fields"

        res.utils.logger.debug(f"update schema payload: {payload}")
        return safe_http.request_post(
            uri,
            json=payload,
            headers=get_airtable_request_header(),
            timeout=AIRTABLE_TIMEOUT,
        ).json()

    def get_field_configuration(self, name):
        # Sirsh, dont see this referred to anywhere so doesnt need to be cache-protected. Could possibly remove this func?
        root = f"{AirtableConnector.API_URI}/meta/bases/{self.base_id}/tables/{self.table_id}/fields"
        data = safe_http.request_get(
            root, headers=get_airtable_request_header(), timeout=AIRTABLE_TIMEOUT
        ).json()
        tabs = data["tables"]
        for table in tabs:
            if table["id"] == "tblWMyfKohTHxDj3w":
                break
            else:
                table = None

        for field in table["fields"]:
            if field["name"] == "Meta.ONE Sizes Ready":
                break
            else:
                field = None
        return field

    def download_attachments(self, field, target):
        # Sirsh, dont see this referred to anywhere so doesnt need to be cache-protected. Could possibly remove this func?
        uri = ""
        data = safe_http.request_get(
            uri, headers=get_airtable_request_header(), timeout=AIRTABLE_TIMEOUT
        ).json()
        return data

    def download_attachments(self, uri):
        data = safe_http.request_get(
            uri, headers=get_airtable_request_header(), timeout=AIRTABLE_TIMEOUT
        ).json()
        return data

    def update_multiselects_column(self, name, options):
        # Sirsh, dont see this referred to anywhere so doesnt need to be cache-protected. Could possibly remove this func?
        root = f"{AirtableConnector.API_URI}/meta/bases/{self.base_id}/tables/{self.table_id}/fields"

        data = self.get_field_configuration(name)

        return data

        s = data["options"]["choices"]

        for o in options:
            s.append({"name": o})

        data["options"]["choices"] = s

        return safe_http.request_patch(
            root, headers=get_airtable_request_header(), json=data
        )

    def get_key_record_lookup(self):
        try:
            return dict(
                self.to_dataframe(fields=self._key_field)[
                    ["record_id", self._key_field]
                ][[self._key_field, "record_id"]].values
            )
        except:
            logger.debug(
                f"Unable to select key field from which suggests that the airtable table is empty"
            )
            return {}

    def get_record_key_lookup(self):
        return {v: k for k, v in self.get_key_record_lookup()}

    def _repr_html_(self):
        return f"{self._base._base_id}/{self._table_id} ({self._friendly_name})"

    @staticmethod
    def _get_extended_field_metadata(table_id):
        """

        Load our custom metadata from the fields table in our Metabase
        https://airtable.com/tblQiFyKtjnxdG1Rw

        """

        connector = AirtableConnector()
        fields_metadata = connector[FIELDS_BASE][FIELDS_TABLE]
        res = fields_metadata.to_dataframe(
            fields=FIELDS_TABLE_COLUMNS,
            filters="FIND('" + table_id + "', {Table ID})",
        )
        if len(res) == 0:
            return None

        if "webhook_default_profile" not in res.columns:
            res["webhook_default_profile"] = False
        else:
            res["webhook_default_profile"] = res["webhook_default_profile"].map(
                lambda x: str(x).lower() == "true"
            )

        # this is for string matching
        if "res.Network Field Type" in res.columns:
            res["field_type_str"] = res["res.Network Field Type"].map(str).fillna("")
        else:
            res["field_type_str"] = ""

        map_issue_types = {
            "multilineText": "issue_reason",
            "multipleSelects": "issue_tags",
            "checkbox": "is_issue_open",
        }

        def _map_issue(row):
            if "res.Network Field Type" in row:
                if (
                    "Issue" in str(row["res.Network Field Type"])
                    and row["Type"] in map_issue_types
                ):
                    return map_issue_types[row["Type"]]
            return None

        res["issue_type"] = res.apply(_map_issue, axis=1)

        return res

    def get_extended_field_metadata(self):
        """

        Merge the airtable core metadata with what we add to it in the fields table in our meta base

        """

        # we can get the deep type from formula and rollups if we have stored it in our table metadata
        def get_return_type(row):
            try:
                c = row["Config"]
                return json.loads(c)["options"]["result"]["type"]
            except:
                return row["Type"] if "Type" in row else None

        metadata = Airtable._get_extended_field_metadata(self._table_id)

        if metadata is None:
            metadata = pd.DataFrame(self._field_metadata)
            metadata["webhook_default_profile"] = None
            metadata["ID"] = metadata["id"]
        else:
            metadata = pd.merge(
                self._field_metadata,
                metadata,
                left_on=["name"],
                right_on="Name",
                how="left",
            )

        if "ID" not in metadata.columns:
            metadata["ID"] = None

        metadata["is_valid"] = metadata["ID"].isin(metadata["id"])
        metadata["return_type"] = metadata.apply(get_return_type, axis=1)
        metadata["clean_field_name"] = metadata["name"].map(formatting.clean_field_name)

        return metadata

    def refresh_invalid_webhook(self, assume_all=False):
        logger.info("Loading the extended field metadata")
        metadata = self.get_extended_field_metadata()

        if assume_all:
            metadata["webhook_default_profile"] = True

        return self.webhook.refresh_invalid_webhook_from_metadata(metadata)

    def reset_webhook(self):
        metadata = self.get_extended_field_metadata()
        return self.webhook.refresh_invalid_webhook_from_metadata(
            metadata=metadata, create_if_not_exists=True
        )

    @property
    def webhook(self):
        return self._webhook_session

    @property
    def key_field(self):
        f = self.fields
        return f[f["is_key"]].iloc[0]["name"]

    @property
    def fields(self):
        return self._field_metadata

    @property
    def base_id(self):
        return self._base._base_id

    @property
    def table_id(self):
        return self._table_id

    @property
    def table_name(self):
        return self._friendly_name

    @property
    def base_name(self):
        return self._base_name

    def _key_lookup_predicate(self, keys):
        return AirtableConnector.make_key_lookup_predicate(keys, self.key_field)

    def has_field(self, f):
        return f in list(self.fields["name"])

    def get_record_id(self, key):
        pred = self._key_lookup_predicate([key])
        # is user_modified_at date in the data add this to the predicate
        mapping = self.to_dataframe(fields=[self._key_field], filters=pred)[
            ["record_id", self.key_field]
        ]
        if len(mapping):
            return dict(mapping.values).get(key)

    def key_lookup(self, keys=None, fields=None, user_modified_at=None):
        """
        lookup particular fields
        """

        if fields:
            if not isinstance(fields, list):
                fields = [fields]
            if self.key_field not in fields:
                fields.append(self.key_field)
            fields.append(self.key_field)

        pred = self._key_lookup_predicate(keys) if keys else None
        # is user_modified_at date in the data add this to the predicate
        return self.to_dataframe(fields=fields, filters=pred)

    def get_table_url(self):
        """
        Tis is used for interacting normally - not the meta api
        """
        root = f"https://api.airtable.com/v0/{self.base_id}/{quote(self.table_id)}"
        return root

    def _get_record_url(self, record_id):
        """Builds URL with record id"""
        table_url = self.get_table_url()
        return f"{table_url}/{record_id}"

    def get_record_link(self, record_id):
        return f"https://airtable.com/{self.base_id}/{self.table_id}/{record_id}?blocks=hide"

    # TODO: may need retry wrapper
    def iterator(self, fields=None, filters=None, view=None, **options):
        last_update_since_hours = options.get("last_update_since_hours")
        last_modified_field = options.get("last_modified_field", "Last Updated At")

        if last_update_since_hours:
            # currently only supports Last Updated At time stamp comparator date
            add_filters = (
                f'IS_AFTER({{{last_modified_field}}}, DATETIME_PARSE("{res.utils.dates.minutes_ago(last_update_since_hours*60).isoformat()}"))'
                if last_update_since_hours * 60
                else None
            )
            filters = f"AND({filters, add_filters})" if filters else add_filters

        for record in self._base._get_client().iterate(
            self._table_id, fields=fields, filter_by_formula=filters, view=view
        ):
            d = record["fields"]
            d["record_id"] = record["id"]
            d["__timestamp__"] = record["createdTime"]
            yield d

    def _unstacked_iterator(self, fields=None, filters=None, **options):
        """
        This is an unstacking of the rows that we get from airtable
        We append on metadata about table and base onto each cell/value pair
        We try to match the same schema that is used everywhere for cell values in airtable
        """

        def make_record(k, v):
            return {
                "record_id": record_id,
                "table_id": self._table_id,
                "base_id": self.base_id,
                "timestamp": timestamp,
                # if someone changed the table this may fail. not sure if its a race condition
                "field_id": field_lookup[k],
                "column_name": k,
                "cell_value": v,
                "user": None,
            }

        for rec in self.iterator(fields, filters, **options):
            record_id = rec.pop("record_id")
            timestamp = rec.pop("__timestamp__")
            field_lookup = dict(
                zip(self._field_metadata["name"], self._field_metadata["id"])
            )

            for k, v in rec.items():
                yield make_record(k, v)

    def updated_rows(
        self,
        minutes_ago=None,
        hours_ago=None,
        since_date=None,
        last_modified_field="Last Modified",
        fields=None,
    ):
        """
        using a suitably defined last modified date field, check for any rows updated since N hours ago

        pass since_date or one of the hours or minutes. if since date is passed that will be used

        """

        if since_date:
            filters = f'IS_AFTER({{{last_modified_field}}}, DATETIME_PARSE("{since_date.isoformat()}"))'
        else:
            minutes_ago = minutes_ago or (60 * hours_ago)
            filters = (
                f'IS_AFTER({{{last_modified_field}}}, DATETIME_PARSE("{res.utils.dates.minutes_ago(minutes_ago).isoformat()}"))'
                if minutes_ago
                else None
            )
        return self.to_dataframe(filters=filters, fields=fields)

    def changes(self, cursor=None, require_lock=False):
        """
        Streaming changes from the webhook
        The context manager can be used to write session info
        There is currently an experimental feature to lock sessions so that we
        Do not send events for changes multiple times
        """
        self._webhook_session._check_owns = require_lock

        with self._webhook_session as hook:
            cursor = cursor if cursor is not None else hook.our_cursor
            logger.debug(f"Using webhook cursor {cursor}...")

            results = [
                change
                for change in self._changes(webhook_id=hook.webhook_id, cursor=cursor)
            ]

            df = pd.concat(results) if len(results) > 0 else pd.DataFrame()
            if len(df) > 0:
                self._webhook_session._cursor = df["cursor"].max()

            return df

    def _changes(self, webhook_id, cursor, timestamp_watermark=None):
        for change, cursor in stream_cell_changes(self.base_id, webhook_id, cursor):
            yield change

    def changes_as_latest(self, cursor=0, schema=None, drop_not_on_schmea=True):
        """
        Get all changes and pivot on the lastest value to recover a snapshot
        If schema is supplied we can load the type and rename the columns and optionally drop things that are not defined on the schema
        """
        data = self.changes(cursor=cursor)
        # keep the latest values for the cell
        data = data.sort_values("timestamp").drop_duplicates(
            subset=["record_id", "column_name"], keep="last"
        )
        data = data.pivot("record_id", "column_name", "cell_value")
        # add the timestamp metadata
        ts = data[["record_id", "timestamp"]].groupby("record_id").max()
        data = data.pivot("record_id", "column_name", "cell_value").join(ts)

        if schema == None:
            return data

        # TODO: deprecated this
        schema = get_schema_by_name(schema)
        fields = dict(
            pd.DataFrame(schema["fields"])[["airtable_field_name", "name"]]
            .dropna()
            .values
        )

        if drop_not_on_schmea:
            return dataframes.rename_and_whitelist(columns=fields)

        return data.rename(columns=fields)

    def update_records_many(self, records):
        for record in records:
            self.update_record(record)

    def update_record(self, record, **options):
        """

         update for airtable via kafka
        {
            "submitting_app": "kafka_to_airtable_testing",
            "submitting_app_namespace": "res-infrastructure",
            "airtable_base": "appUjNEiW5uZDjCdI",
            "airtable_table": "tblAiAn1GXmYoom1I",
            "airtable_column": "Status",
            "airtable_record": "recboAnjBfBC7XBbi",
            "new_value": "In progress"
            }


        """
        record = dict(record)
        client = self._base._get_client()
        field_types = dict(self.fields[["name", "type"]].values)
        use_kafka_to_update = options.get("use_kafka_to_update")

        response = None
        timestamp = record.pop("__timestamp__") if "__timestamp__" in record else None
        record_id = record.pop("record_id") if "record_id" in record else None
        app = os.environ.get("RES_APP", options.get("RES_APP"))
        namespace = os.environ.get("RES_NAMESPACE", options.get("RES_NAMESPACE"))

        if use_kafka_to_update:
            kafka = res.connectors.load("kafka")
            for k, v in record.items():
                # TODO: im not sure if this supports values as lists but it really should
                if isinstance(v, list):
                    v = str(v)
                # publish non null values
                if isinstance(v, list) or pd.notnull(v):
                    payload = {
                        "submitting_app": app or "res-data",
                        "submitting_app_namespace": namespace or "res",
                        "airtable_table": self.table_id,
                        "airtable_base": self.base_id,
                        "airtable_record": record_id,
                        "airtable_column": k,
                        "new_value": v,
                    }
                    response = kafka[
                        "res-infrastructure.kafka_to_airtable.airtable_updates"
                    ].publish(payload, use_kgateway=True)

        else:
            # we dont update fields like this if they exist
            record = {
                k: v
                for k, v in record.items()
                if field_types.get(k) not in ["lastModifiedTime"]
            }

            if pd.isnull(record_id):
                url = self.get_table_url()
                response = safe_http.request_post(
                    url,
                    json={"fields": record, "typecast": True},
                    headers=get_airtable_request_header(),
                    timeout=AIRTABLE_TIMEOUT,
                )

                logger.debug("created new record")
            else:
                url = self._get_record_url(record_id)

                logger.debug("updated existing record")
                response = requests.patch(
                    url,
                    json={"fields": record, "typecast": True},
                    headers=get_airtable_request_header(),
                    auth=None,
                    timeout=AIRTABLE_TIMEOUT,
                )

                # if its 404 i believe its fair game to push the id again but
                if response.status_code == 404:
                    url = self.get_table_url()
                    # test the above flow actually modifies the record id
                    logger.debug(
                        "actually creating a new record because the record id does not exist"
                    )
                    response = requests.post(
                        url,
                        json={
                            "fields": {
                                k: v for k, v in record.items() if k != "record_id"
                            },
                            "typecast": True,
                        },
                        headers=get_airtable_request_header(),
                        auth=None,
                        timeout=AIRTABLE_TIMEOUT,
                    )

        return response

    def get_record(self, record_id):
        url = self._get_record_url(record_id)
        return safe_http.request_get(
            url,
            headers=get_airtable_request_header(),
            timeout=AIRTABLE_TIMEOUT,
        ).json()

    def update_with_retries(self, record, retry_waits=[50, 50, 50, 50]):
        result = {"status": "Failed", "id": record.get("id")}
        known_fields = list(self.fields["name"]) + ["record_id"]

        known_field_filter = lambda d: {k: v for k, v in d.items() if k in known_fields}

        for i, w in enumerate(retry_waits):
            a = res.utils.dates.utc_now()
            b = None
            try:
                send_record = known_field_filter(record)
                # here in this higher level method we only submit fields we know about
                result = self.update_record(send_record).json()
                b = res.utils.dates.utc_now()
                result["status"] = "ok"
                # print(record)
                break
            except Exception as ex:
                # some status codes we should not retry e.g. 404
                result["errors"] = res.utils.ex_repr(ex)
                # sleep(w)

        result["retry"] = i
        result["response_time"] = (b - a).total_seconds() if b else None
        result["mode"] = "create" if not record.get("record_id") else "update"

        # latency metrics to statsd
        return result

    def delete_with_retries(self, record, retry_waits=[50, 50, 50, 50]):
        result = {"status": "Failed", "id": record.get("id")}
        for i, w in enumerate(retry_waits):
            a = res.utils.dates.utc_now()
            b = None
            try:
                result = self.delete_record(record["record_id"])
                result = result.json()
                b = res.utils.dates.utc_now()
                result["status"] = "ok"
                break
            except Exception as ex:
                result["errors"] = res.utils.ex_repr(ex)
                # sleep(w)

        result["retry"] = i
        result["response_time"] = (b - a).total_seconds() if b else None
        result["mode"] = "delete"

        # latency metrics to statsd
        return result

    def delete_record(self, record_id):
        return self._base._get_client().delete(self._table_id, record_id)

    def update(
        self,
        df,
        mode="simple",
        skip_control_fields=["created", "modified"],
        **kwargs,
    ):
        """
        Sync push changes from dataframe to airtable
        This is not to be used with legacy tables and is used only for specific tables that match a convention for what should be updated

        We need to get our keys and the corresponding airtable record ids. When new, we insert and when modified we update by record id
        """

        if isinstance(df, dict):
            return self.update_record(df, **kwargs)

        # this could be useful to allow only creates on airtable so we send users data they can update status fields on and we take the updates but dont expect they need updates
        create_only = kwargs.get("create_only", True)

        if mode != "simple":
            # test flow
            key_field = self._key_field
            existing_keys = self.get_key_record_lookup()
            existing_fields = list(self.fields["name"].values)
            # add the record id that airtable uses in case we dont have it in our dataframe for all records by key

            # only select fields that are in airtable
            data_to_mirror = (
                df[
                    [
                        c
                        for c in df.columns
                        if c in existing_fields and c not in skip_control_fields
                    ]
                ]
                .reset_index()
                .drop("index", 1)
            )

            data_to_mirror["record_id"] = data_to_mirror[key_field].map(
                lambda x: existing_keys.get(x)
            )

        data_to_mirror = df

        logger.info(
            f"Upserting {len(data_to_mirror.columns)} fields including record_id for {len(data_to_mirror)} records on table {self.table_name}"
        )
        for record in data_to_mirror.to_dict("records"):
            record_id = record.pop("record_id")
            if pd.isnull(record_id):
                self._base._get_client().create(self._table_id, record)
            else:
                if not create_only:
                    self._base._get_client().update(self._table_id, record_id, record)

        logger.info("done")

    def read_all(
        self,
        entity_name=None,
        partitions_modified_since=None,
        drop_not_on_schmea=True,
    ):
        LATEST_LOCATION = f"s3://{AIRTABLE_LATEST_S3_BUCKET}/{self._base._base_id}/{self._table_id}/.latest"

        # assume exists for now
        logger.info(f"Loading all data from {LATEST_LOCATION}...")
        data = pd.read_parquet(LATEST_LOCATION)
        field_names = formatting.get_field_names(data)
        data = data.pivot("record_id", "field_id", "cell_value")
        logger.info("Mapping datatypes using Fields metadata for pivoted table")
        data = formatting.map_field_types(self, data)
        # the pivot has columns of field_ids, these are mapped to names (if there are multiple mappins we append .1, .2 etc for now)
        data = data.rename(columns=field_names)

        # we map to our schema if supplied
        if entity_name:
            schema = get_schema_by_name(entity_name)
            fields = dict(
                pd.DataFrame(schema["fields"])[["airtable_field_name", "name"]]
                .dropna()
                .values
            )

            if drop_not_on_schmea:
                return dataframes.rename_and_whitelist(columns=fields)

            return data.rename(columns=fields)

        return data

    ### #################################### ###
    #   The airtable file migration flow:      #
    #   - take_snapshot                        #
    #   - make_typed_table                     #
    #   - update_record_partitions             #
    #   - trigger snowpipe                     #
    ### #################################### ###

    def take_snapshot(self, merge_latest=False, queue_content_downloads=False):
        """
        Take a full snapshot of an airtable and store it on S3 as a partial
        and also merged into the fully history
        """
        s3 = S3Connector()

        LATEST_LOCATION = f"s3://{AIRTABLE_LATEST_S3_BUCKET}/{self._base._base_id}/{self._table_id}/.latest"

        SNAPSHOT_LOCATION = f"s3://{AIRTABLE_VERSIONED_S3_BUCKET}/{self._base._base_id}/{self._table_id}/.snapshot"

        logger.info(f"Loading table {self._table_id} ...")

        # NB: it is important that we are calling timestamp is the airtable create and not modified record because we use it as a fixed partition key
        df = pd.DataFrame(self._unstacked_iterator())
        logger.info(f"saving {len(df)} snapshot rows to {SNAPSHOT_LOCATION} ...")

        df = pd.DataFrame(self._unstacked_iterator())
        df["cell_value"] = df["cell_value"].map(str)
        # TODO: replace this with s3_save for error checking
        df.to_parquet(SNAPSHOT_LOCATION)

        if merge_latest:
            if s3.exists(LATEST_LOCATION):
                logger.info(f"merging snapshot into {LATEST_LOCATION}")
                latest = pd.read_parquet(LATEST_LOCATION)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                latest["timestamp"] = pd.to_datetime(latest["timestamp"])
                latest = merge_table_sets(latest, df)
                latest.to_parquet(LATEST_LOCATION)
                df = latest
            else:
                df.to_parquet(LATEST_LOCATION)

        return df

    def make_typed_table(
        self,
        latest=None,
        add_record_timestamp=True,
        expand_nested=False,
        clean_latest_field_names=True,
    ):
        """
        This an important step that decides the schema information for the aitable mirror.
        Once cleaned the data can be moved to Athena and Snowflake
        There are numerous metadata uses and assumptions here for checking names and types which must be checked for consistency
        If we get this right, generating snowflake and athena schema is easy enough - we save parquet with type info except for Variant maps and lists which we deal with after
        we attempt to resolve many fields named different things are multiple field(ids) mapped to the same name

        @latest: the latest full airtable with all historic data or a subset of it for generating a partition
        @add_record_timestamp: by default we add timestamp(s) on each row as metadata for managing deltas
        @expand_nested: (TODO:) in some cases we expand out nested tables
        @clean_latest_field_names: apply a standard rule to remove funny characters etc. from field names

        """
        LATEST_LOCATION = f"s3://{AIRTABLE_LATEST_S3_BUCKET}/{self._base._base_id}/{self._table_id}/.latest"

        if latest is None:
            logger.info(f"No latest data provided, downloading from {LATEST_LOCATION}")
            latest = pd.read_parquet(LATEST_LOCATION)

        if clean_latest_field_names:
            logger.info(
                "Cleaning all field names - this string manipulation can take a moment for large sets..."
            )
            latest["column_name"] = latest["column_name"].map(
                formatting.clean_field_name
            )

        # the default snapshot storage is cell-orientated with metadata - here we pivot out a row orientated table
        piv = latest.pivot("record_id", "field_id", "cell_value")

        logger.info("Mapping datatypes using Fields metadata for pivoted table")
        # this *key step* tries to convert string cell values to correct cell types including managing aitable type semantics
        # TODO: spend more time on check all types match what is done today (same goes for col names)
        piv = formatting.map_field_types(self, piv)
        # the pivot has columns of field_ids, these are mapped to names (if there are multiple mappins we append .1, .2 etc for now)
        piv = piv.rename(columns=formatting.get_field_names(latest))

        # TODO: we must use the created date of the row and not the modified as the partition key
        # need to recheck the convention for timestamp column names
        if PARTITION_KEY_SORT_COLUMN not in latest.columns:
            latest[PARTITION_KEY_SORT_COLUMN] = latest["timestamp"]
        if not is_datetime(latest[PARTITION_KEY_SORT_COLUMN]):
            logger.info(
                f"Casting timestamp column - this could be done on saving though"
            )
            latest[PARTITION_KEY_SORT_COLUMN] = pd.to_datetime(
                latest[PARTITION_KEY_SORT_COLUMN]
            )

        row_timestamps = pd.DataFrame(
            latest.pivot("record_id", "field_id", PARTITION_KEY_SORT_COLUMN).T.max(
                skipna=True
            ),
            columns=[PARTITION_KEY_SORT_COLUMN],
        )

        return piv.join(row_timestamps)

    def _update_partition_file(self, path, data, replace=False):
        # TODO: we assume for now replacement: but we could safely merge with an existing partition instead
        # we should only write new files if they are different - use the data to determine if changed??

        s3 = S3Connector()
        existing = None
        if s3.exists(path) and not replace:
            logger.info(f"opening existing partition file {path}")
            existing = s3.read(path)
            chk_sum_existing = pd.util.hash_pandas_object(existing).sum()
            chk_sum_current = pd.util.hash_pandas_object(data).sum()

            # no change, dont update the file
            if chk_sum_existing == chk_sum_current:
                return

            logger.info(
                f"replace is set to [{replace}] and the partition already exists. Merging {len(existing)} records to {len(data)} records and saving to path {path}"
            )
            data = pd.concat([existing, data])

        # coerce types to match existing or to be the most specific - generall

        if not is_datetime(data[PARTITION_KEY_SORT_COLUMN]):
            data[PARTITION_KEY_SORT_COLUMN] = pd.to_datetime(
                data[PARTITION_KEY_SORT_COLUMN], utc=True
            )

        data = data.sort_values(PARTITION_KEY_SORT_COLUMN)
        # NB: this only makes sense because we store partitions indexed by record ids
        data = data[~data.index.duplicated(keep="last")]

        try:
            data = dataframes.remove_object_types(data)
            data.to_parquet(path)
        except Exception as ex:
            fail_path = path + ".failed.csv"
            logger.warn(
                f"Failed to save a partition with proper types {repr(ex)} - writing to {fail_path}"
            )
            data.to_csv(fail_path)
            # mode fail
            raise ex

    def update_record_partitions(
        self,
        data,
        granularity="D",
        partition_date=PARTITION_KEY_SORT_COLUMN,
        watermark=None,
        replace=True,
    ):
        """
        We use both the creation-timestamp for the partition and the latest added-time (to latest data) as a watermark
        The snowpipe process can check which partitions to invalidate using this metadata

        When we are done, we refresh the Glue partition too
        We could maybe split between our partitions and snowflake's if snowpipe is working too hard to monitor files
        But avoiding this provides a single set of partition of files to manage

        @data: the full change set that we need to build/re-build partitions (which ones were built must be traceable from latest)
        @granularity: the chunking process for building partitions. By default each day based on the created timestamp of records is used

        returns:> paths to updated partitions

        This output parameter can be used to coordinate with snowpipe which already knows the table id and name from the flow session

        if doing a full merge, it is safe to set replace=true and this rebuilds the parquet file e.g. types
        But if we are partial update we could lose data so set replace = False
        """

        ROOT_LOCATION = f"s3://{AIRTABLE_LATEST_S3_BUCKET}/{self._base._base_id}/{self._table_id}/partitions/"

        def make_partition(ts):
            # TODO: what is the partitioning logic - assume daily for now
            try:
                formatted = ts.strftime("%Y-%m-%d")
            except:
                formatted = dates.utc_now().strftime("%Y-%m-%d")
            return f"{ROOT_LOCATION}{formatted}/part0.parquet"

        # split out each partition, overwriting any files that would be there
        data["__partition__"] = data[partition_date].map(make_partition)
        # not sure how i want to handle row ids yet
        data["__row_id__"] = data.index
        for path, records in data.groupby("__partition__"):
            self._update_partition_file(path, records, replace=replace)

        dataframes.push_schema_to_glue(data, ROOT_LOCATION, metadata=None)

        result = list(data["__partition__"].unique())

        logger.debug(f"updated {len(result)} partitions")

        return result

    def read_and_map_to_schema(
        self, schema_name, filters=None, provider=None, add_field=None
    ):
        """
        for legacy airtables we can read any table and map it to our schema
        the meta data stores our fields with the airtable table/field mapping
        here we assume that the airtable field name is listed in the schema or we dont try to load it
        - TODO: we can also add_fields which are read and renamed

        """
        # we need to generalizse how we load this provider. use connectors maybe with DI
        from res.connectors.dgraph import DgraphConnector

        provider = provider or DgraphConnector
        schema = provider.try_load_meta_schema(name=schema_name)
        f = pd.DataFrame(schema["fields"])[["airtable_field_name", "name", "key"]]
        f["name"] = f["name"].fillna(f["key"])
        fields = dict(f[["airtable_field_name", "name"]].dropna().values)
        data = self.to_dataframe(fields=list(fields.keys()), filters=filters).rename(
            columns=fields
        )
        return data

    def read_and_map(self, mapping, filters=None):
        """
        Simple helper to read fthe keys from airtable and rename them to the values
        """
        return self.to_dataframe(fields=list(mapping.keys()), filters=filters).rename(
            columns=mapping
        )
