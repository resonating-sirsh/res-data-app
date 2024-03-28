"""
Module to maintain airtable queues

generator, consume off kafka messages
handler, try to update airtable with the payload - write state
generator, retry dropped messages

"""
import res
import json
from res.utils.strings import compress_string, decompress_string
from datetime import datetime
from res.connectors.airtable.AirtableConnector import (
    AIRTABLE_ONE_PLATFORM_BASE,
    RES_ENV,
)
import traceback
import stringcase

from schemas.pydantic.common import uuid_str_from_dict


GET_DROPPED_MESSAGES = """"""
GET_DROPPED_MESSAGES_BY_NAME = """"""
UPDATE_QUEUE_STATE = """
mutation update_airtable_queue_audit($state: infraestructure_airtable_updates_insert_input = {}) {
  insert_infraestructure_airtable_updates_one(object: $state, on_conflict: {constraint: airtable_updates_pkey, update_columns: [compressed_payload, trace, hid, rid, metadata, processed_at]}) {
    id
    rid
  }
}
"""
UPDATE_QUEUE_STATE_REMOVED_KEYS = """ """


class AirtableQueue:
    def __init__(self, table_name_or_id, base_id=None, on_init_fail="raise", **kwargs):
        """
        the airtable queue connects to a particular table in a particular base
        it is expected that there is a separate base for each env - the user can pass any base but by default we have a pair for one platform dev and prod
        if the constructor fails it is because the tables or base have not been created
        while we could create the tables automatically it is recommended that we do this manually using the refresh_schema_from_type method and a pydantic type
        """

        # experimental: dont move to top circular
        from res.flows.meta.ONE.contract import ContractVariables

        base_id = base_id or AIRTABLE_ONE_PLATFORM_BASE
        self._table_id_or_name = table_name_or_id

        self._base_id = base_id
        self._initialized = False
        # experimental: should we make contract variables first class citizens of the queue
        self._contract_variables = ContractVariables(base_id=base_id)
        try:
            self._init()
        except:
            if on_init_fail == "raise":
                raise

    def _init(self):
        try:
            self._base = res.connectors.load("airtable")[self._base_id]
            self._table = self._base[self._table_id_or_name]

            self._key_field = self._table.key_field
            self._initialized = True

        except:
            res.utils.logger.warn(
                f"Failed to initialize airtable queue for {self._base_id}/{self._table_id_or_name} - for ENV {RES_ENV}"
            )
            raise Exception(
                f"Airtable queue init failed - {self._base_id}/{self._table_id_or_name} - for ENV {RES_ENV}"
            )

    def publish(self, payload, **kwargs):
        refresh_schema = kwargs.get("refresh_schema", False)
        # some adornments, as in new fields that are de-normalized, may be possible here but easier to just update the kafka topics to be "complete"
        return self.try_update_airtable(
            **payload.get_airtable_update_options(), refresh_schema=refresh_schema
        )

    @staticmethod
    def get_airtable_field_type(o, is_date_type=False, is_attachment_type=False):
        """
        an object or a type can be passed
        """
        ftype = o if isinstance(o, type) else type(o)

        d = {}

        if is_attachment_type:
            d["type"] = "multipleAttachments"
        elif ftype is list:
            d["type"] = "multipleSelects"
            d["options"] = {"choices": [{"name": "*"}]}
        elif ftype is int:
            d["type"] = "number"
            d["options"] = {"precision": 0}
        elif ftype is float:
            d["type"] = "number"
            d["options"] = {"precision": 2}
        elif ftype is bool:
            d["type"] = "checkbox"
            d["options"] = {"icon": "check", "color": "greenBright"}

        elif ftype is datetime or is_date_type:
            d["type"] = "dateTime"
            d["options"] = {
                "dateFormat": {"name": "iso"},
                "timeFormat": {"name": "24hour"},
                "timeZone": "utc",
            }
        elif ftype is str:
            d["type"] = "singleLineText"
        elif ftype is dict:
            d["type"] = "singleLineText"
        else:
            raise Exception(f"Could not handle the case for type {ftype}")

        return d

    @staticmethod
    def get_airtable_field_spec_from_dict(input_dict, **kwargs):
        # TODO: hard code attachments as an attachment type
        attachment_fields = kwargs.get("attachment_fields", ["Attachments"])
        fields = []

        for k, v in input_dict.items():
            is_date = k in ["Created At", "Modified At"] or k in kwargs.get(
                "date_fields", []
            )
            is_attachment = k in attachment_fields

            d = AirtableQueue.get_airtable_field_type(
                v, is_date_type=is_date, is_attachment_type=is_attachment
            )
            d["name"] = k
            fields.append(d)

        return fields

    @staticmethod
    def refresh_schema(airtable_schema, airtable_base):
        one_platform_airtable = res.connectors.load("airtable")[airtable_base]

        table_name = airtable_schema["table_name"]
        res.utils.logger.info(f"Refreshing schema for table {table_name}")

        key_field = airtable_schema["key_field"]

        # todo we could pass the extra fields in here actually and do it onece
        table_id = one_platform_airtable.create_new_table(
            table_name, key_name=key_field
        )["id"]
        # TODO refresh meta data after create table in a more sensible way

        t = one_platform_airtable[table_id]
        existing_fields = list(t.fields["name"])
        for name, f in airtable_schema["fields"].items():
            f["name"] = name
            # we can do some sort of migration separately if the schema evolves by rename and migrate fields - not supported for now
            if f["name"] not in existing_fields:
                res.utils.logger.debug(
                    f"Updating field [{f['name']}] on table [{table_name}] using {f} as it is not in the existing fields {existing_fields}"
                )
                t.update_field_schema(f)
                # type warnings here
        # refresh table fields
        one_platform_airtable.reload_cache(table_id=table_id)
        # refresh the base
        one_platform_airtable.reload_cache()

        res.utils.logger.info(f"Airtable schema synced on [{table_name}]")
        return airtable_schema

    def update_schema_refs(self, schema):
        """
        sadly the schema knows nothing about actually table ids
        we would like to use names of tables but airtable always asks us for ids which is annoying
        for example if you are referring to a linked table in a base, you need to provide the table id instead of the name
        """
        for k, v in dict(schema["fields"]).items():
            if v["type"] == "multipleRecordLinks":
                table = v["options"]["linkedTableId"]
                if table[:3] != "tbl":
                    v["options"]["linkedTableId"] = self._base.get_table_id_from_name(
                        table
                    )
                schema["fields"][k] = v
        return schema

    @staticmethod
    def refresh_schema_from_type(atype, airtable_base=AIRTABLE_ONE_PLATFORM_BASE):
        """
        Get the schema from the type and upsert
        """
        airtable_schema = atype.airtable_schema()

        AirtableQueue.refresh_schema(airtable_schema, airtable_base=airtable_base)
        return airtable_schema

    @staticmethod
    def refresh_schema_from_record(
        record,
        table_name=None,
        key_field=None,
        airtable_base=AIRTABLE_ONE_PLATFORM_BASE,
        **kwargs,
    ):
        """
        DEPRECATE
        Passing a table name and field info in the proper format
        we should do this when we fail to update airtable because of a certain type of error

        the record passed in can be either a map of property names to values or property names to types
        either can be used to determine the airtable schema
        """
        key = key_field
        field_info = AirtableQueue.get_airtable_field_spec_from_dict(record)

        if key is None:
            key = record.airtable_primary_key_field
        if table_name is None:
            table_name = record.airtable_table_name
        # reload the base
        one_platform_airtable = res.connectors.load("airtable")[airtable_base]

        _ = one_platform_airtable.create_new_table(table_name, key_name=key_field)
        # TODO refresh meta data after create table in a more sensible way
        one_platform_airtable = res.connectors.load("airtable")[airtable_base]

        t = one_platform_airtable[table_name]
        existing_fields = list(t.fields["name"])
        for f in field_info:
            # we can do some sort of migration separately if the schema evolves by rename and migrate fields - not supported for now
            if f["name"] not in existing_fields:
                res.utils.logger.debug(
                    f"Updating field [{f['name']}] on table {table_name} using {f}"
                )
                t.update_field_schema(f)
                # type warnings here

        # reload the field meta data in cache
        t.reload_cache()

        res.utils.logger.info("Done")

        return field_info

    def try_update_airtable_record(self, record_typed, **kwargs):
        if not self._initialized or kwargs.get("refresh_schema"):
            res.utils.logger.warn(
                f"Because the table does not exist yet, we will create it for the record using conventions or config to name the table"
            )
            schema = record_typed.airtable_schema()
            # we need to do this because the schema knows nothing about actually able ids which sometimes we need e.g. linked tables ??
            schema = self.update_schema_refs(schema)
            AirtableQueue.refresh_schema(schema, airtable_base=self._base_id)
            self._init()

        # the airtable attachment fields can be configured or we just try to use the data as given
        kwargs["attachment_fields"] = getattr(
            record_typed, "airtable_attachment_fields", []
        )

        return self.try_update_airtable(
            record_typed.airtable_dict(contract_variables=self._contract_variables),
            **kwargs,
        )

    def try_update_airtable(self, record, **kwargs):
        """
        Args:
         record: a dictionary usually mapped from  type at high level

        push an update to airtable from a kafka message
        we can burn in the kafka queue before running this as there are easier ways to update in batch

        TODO - we can internally push these on some intermediate queue to throttle or guarantee delivery
        """

        # print(record)

        key_field = self._key_field
        table = self._table
        table_name = table.table_id

        s3 = res.connectors.load("s3")

        def _m(v):
            """
            special type mapping for airtable
            """
            if isinstance(v, dict):
                return json.dumps(v)
            return v

        record = {stringcase.titlecase(k): _m(v) for k, v in record.items()}

        def map_attachment_fields(d):
            def _mapit(attachments):
                # not sure about this yet
                if isinstance(attachments, str):
                    attachments = [attachments]
                if not isinstance(attachments, list):
                    return attachments
                # assume these are urls for now or can be mapped to them later
                # assertion here is we are assuming an s3 type url s3://
                for item in attachments:
                    if not isinstance(item, str):
                        raise Exception(
                            f"The attachments must be strings and s3 uris at that: {item}"
                        )
                return [
                    {"url": s3.generate_presigned_url(item)}
                    for item in attachments or []
                ]

            # convention for testing
            attachment_fields = kwargs.get("attachment_fields", ["Attachments"])

            akeys = [
                a
                for a in d.keys()
                if attachment_fields is not None and a in attachment_fields
            ]
            return {k: v if not k in akeys else _mapit(v) for k, v in d.items()}

        source = kwargs.get("source")
        fail_trace = None
        key_value = None
        processed_at = None
        response = None
        try:
            compressed_payload_if_failed = None

            key_value = record[key_field]
            record_id = table.key_cache[key_value]
            if record_id:
                res.utils.logger.debug(f"resolved existing id {key_value}:{record_id}")
                record["record_id"] = record_id
            # TODO make the retry logic better - there are some codes you want to retry and some you dont
            # TODO handle timestamps and coerce

            # TODO swap out keys: we can supply a dictionary of keys to the context or it will go to redis - this could be slow for large sets e.g. all pieces on a roll
            # for pieces there is a large number so the resolution time will be slow. in special cases we could select them from the queue table
            prepared_record = map_attachment_fields(record)
            # we should take all of this stuff out into an as_airtable_dict which would allow for unit testing etc.
            # print(prepared_record)
            # NB: REDIS CONNECTION IS REQUIRED TO CACHE KEYS TODAY: TICKET TO USE COLD STORAGE WHEN NO CACHE AND TO SLOW RELOAD THE CACHE
            if kwargs.get("plan"):
                return prepared_record

            response = table.update_record(prepared_record).json()

            if "id" not in response:
                res.utils.logger.warn(f"Airtable problem {response }")

            record_id = response["id"]
            res.utils.logger.debug(f"caching id from response {response}")
            table.key_cache[key_value] = response["id"]
            if kwargs.get("print_updated_record_uri"):
                res.utils.logger.info(f"updated {table.get_record_link(record_id)}")
            else:
                res.utils.logger.debug(f"updated {table.get_record_link(record_id)}")

            # we actually processed so we can say so
            processed_at = res.utils.dates.utc_now_iso_string()
        except Exception as ex:
            res.utils.logger.warn(
                f"doh! failed to update airtable {traceback.format_exc()}"
            )
            compressed_payload_if_failed = compress_string(json.dumps(record))
            fail_trace = traceback.format_exc()

            # raise ex
        finally:
            try:
                hasura = res.connectors.load("hasura")
                hasura.execute_with_kwargs(
                    UPDATE_QUEUE_STATE,
                    state={
                        "id": res.utils.uuid_str_from_dict(
                            {"key": key_value, "name": table_name}
                        ),
                        "key": key_value,
                        "hid": None,  # pydantic_entity.id,
                        "name": table_name,
                        "rid": record.get("record_id", "no_record_id"),
                        "compressed_payload": compressed_payload_if_failed,
                        # processed at should be None to say this needs processing
                        "processed_at": processed_at,
                        # metadata add the table name for readability and maybe other stuff like the kafka source
                        # todo add the trace that explains the failure
                        "metadata": {
                            "source": source,
                            "table_name": str(
                                table.table_name
                            ),  # table_name is sometimes a dataframe, which cant be json serialised and throws an error
                        },
                        "trace": fail_trace,
                    },
                )
            except Exception as ex:
                # res.utils.logger.error(f"Failed message")
                res.utils.logger.debug(
                    f"Failed to log dropped airtable update for record {record} - {ex}"
                )
        return {}

    def try_purge_airtable_records(self, keys, postgres=None):
        """
        resolve the record ids from cache(ttl) and and remove any records that we dont want in the airtable
        the trick is how we know the keys and this is based on hasura queries defined per queue
        """

        from datetime import datetime
        import traceback
        from tqdm import tqdm

        postgres = postgres or res.connectors.load("postgres")
        now_iso = datetime.now().isoformat()

        res.utils.logger.info(
            f"Purged at date: {now_iso}  - Keys requested to be deleted: ({len(keys)}) {','.join(keys)}"
        )
        # determine the already deleted keys - we dont want to try purging those again
        df_deleted_keys = postgres.run_query(
            "SELECT KEY from infraestructure.airtable_purged_row_keys WHERE table_id = %s ",
            (self._table.table_id,),
            keep_conn_open=True,
        )

        already_deleted = df_deleted_keys["key"].tolist()

        # despite the apparent attracitveness of using redis to load the keys/recordid combos, its far far faster to do a bulk load of the key and record pairs from airtable, as airtable can give them to you in a bulk query response, but redis does not handle bulk reqeusts well
        # and redis is slow for the individual requests (9 per second, ran locally). Crucially we are not usuing airtables data to determine what to delete (that comes from hasura/postgres), but just using it as the key value store for the keys/record lookups.
        # see below for sample code

        # remove those already deleted from keys
        keys = list(set(keys).difference(set(already_deleted)))

        res.utils.logger.info(
            f"Already deleted for this table: {len(already_deleted)}  - Keys left after removing those already deleted: ({len(keys)})"
        )
        removed_keys = []
        cacheKeyMissing = []
        failed_deletes = []

        for key in tqdm(keys):
            record_id = self._table.key_cache[key]

            if record_id:
                try:
                    self._table.delete_record(record_id)
                    # even though we didnt read from the redis cache- we will keep it up to date by deleting from it
                    self._table.key_cache.delete(key)
                    removed_keys.append(key)
                except Exception as ex:
                    res.utils.logger.info(
                        f"{key} failed to delete on {self._table.table_id} - exception: {traceback.format_exc()}"
                    )
                    failed_deletes.append(key)
            else:
                cacheKeyMissing.append(key)

        res.utils.logger.info(
            f"Delete failed for these keys {','.join(failed_deletes)}"
        )
        res.utils.logger.info(
            f"Delete failed for these keys becuase they are not in redis cache: {','.join(cacheKeyMissing)}"
        )
        res.utils.logger.info(
            f"try_purge_airtable_records - Keys Deleted: {','.join(removed_keys)}"
        )

        removed_keys.extend(cacheKeyMissing)
        # build a list of dicts we can directly insert into infraestructure.airtable_purged_row_keys
        dicts_insert = [
            {"table_id": self._table.table_id, "key": k, "purged_at": now_iso}
            for k in removed_keys
        ]

        for d in dicts_insert:
            d["id"] = uuid_str_from_dict(d)

        postgres.insert_records(
            "infraestructure.airtable_purged_row_keys", dicts_insert
        )

        res.utils.logger.info(
            f"{len(dicts_insert)} Purged Keys written to infraestructure.airtable_purged_row_keys"
        )
        return {}

    # use this to load the key lookup from airtable. suitable for large number of rows to be deleted
    #    res.utils.logger.info("loading keys from airtable")
    #     airtable = res.connectors.load("airtable")
    #     df_key_recordids = airtable[self._table.base_id][
    #         self._table.table_id
    #     ].to_dataframe(
    #         fields=[
    #             "One Number",
    #             "Status",
    #         ]  # asking for just the key col returns an error - have to include a random extra column
    #     )

    #     dict_key_recordids = df_key_recordids.set_index("One Number")[
    #         "record_id"
    #     ].to_dict()
    # tehn use this in teh for loop to get the key to delete, instead of redis
    #    record_id = dict_key_recordids.get(key)

    def __api_delete_records(self, keys, plan=False):
        """
        util to purge keys from airtable only and remove the record ids from the cache - for the table
        """
        table = None  # todo - from api

        for key_value in keys:
            rid = table.key_cache[key_value]
            res.utils.logger.info(f"Planned delete {rid}:{key_value}")

            if not plan:
                table.delete_record(rid)
                table.key_cache[key_value] = None

    def retry_failed_records(self):
        """
        resolve the record ids from cache(ttl) and and remove any records that we dont want in the airtable
        the trick is how we know the keys and this is based on hasura queries defined per queue
        """
        hasura = res.connectors.load("hasura")

        dropped = hasura.execute_with_kwargs(
            GET_DROPPED_MESSAGES_BY_NAME, name="table_id"
        )["SOMETHING"]

        for record in dropped:
            payload = decompress_string(record["compressed_payload"])
            succeeded = False
            try:
                response = self._table.update_record(record)
                succeeded = True
                payload["record_id"] = response["id"]
            except:
                res.utils.logger.warn(f"Failing to update again")
            update = {
                # the counter must be update by the mutation
                "record_id": payload.get("record_id"),
                "processed_at": res.utils.dates.utc_now_iso_string()
                if succeeded
                else None,
            }
            try:
                hasura.execute_with_kwargs(UPDATE_QUEUE_STATE, **update)
            except:
                res.utils.logger.warn(
                    f"failing to update state after airtable update attempt"
                )

        return {}

    @staticmethod
    def restore_redis_cache_from_hasura(table_id=None):
        """
        simple script to restore redis keys from hasura if the worst should happen which it did once
        TODO:
         - add a table filter
         - add a full airtable check on the table i.e. get all key:values in airtable and make sure we have them
        """
        from tqdm import tqdm
        import pandas as pd

        Q = """
        query MyQuery {
        infraestructure_airtable_updates {
            id
            key
            name
            rid
        }
        }

        """

        res.utils.logger.info(
            f"Running restore of redis keys from hasura in env [{res.utils.env.RES_ENV}] - make sure you are connected to the correct redis environment!"
        )

        hasura = res.connectors.load("hasura")
        airtable = res.connectors.load("airtable")
        data = pd.DataFrame(
            hasura.execute_with_kwargs(Q)["infraestructure_airtable_updates"]
        )

        bases = """
        query MyQuery {
        infraestructure_airtable_cache_metadata_tables_in_base {
            table_id
            base_id
        }
        }"""
        tables = pd.DataFrame(
            hasura.execute_with_kwargs(bases)[
                "infraestructure_airtable_cache_metadata_tables_in_base"
            ]
        )
        data = pd.merge(data, tables, left_on="name", right_on="table_id")

        # Use this for airtable
        # df = airtable["apps71RVe4YgfOO8q"][table_id].to_dataframe(
        #     fields=["One Number", "Status"]
        # )

        # result_dict = df.set_index("One Number")["record_id"].to_dict()

        if table_id is not None:
            data = data[
                data["name"] == table_id
            ]  # if you only want to refesh one table, use this

        for tab, d in data.groupby("table_id"):
            base = d.iloc[0]["base_id"]
            t = airtable[base][tab]
            res.utils.logger.info(f"Restoring keys for {base}/{tab} ( {t.table_name} )")

            # if using airtable
            # for key in tqdm(result_dict.keys()):
            for r in tqdm(d.to_dict("records")):
                key = r["key"]
                rid = r["rid"]
                if rid:
                    if rid[:3] == "rec":
                        t.key_cache[key] = rid
