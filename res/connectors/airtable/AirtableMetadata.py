# this is just a crud wrapper
# gets metadata from various places such as fields tables or webhook sessions
# i might make it a static class so its easy to unit test and manage
from datetime import timedelta
from . import MongoConnector
from . import METADATA_REPO_NAME
from . import dates
from . import logger
from datetime import datetime, timedelta
import hashlib, json, uuid, base64
import pandas as pd
from ast import literal_eval


def mongo_client():
    import res

    mongo = res.connectors.load("mongo")
    return mongo.get_client()


class AirtableMetadata(object):
    """
    A semi-implemented repository wrapper around metadata
    In future we would like to be able to use something other than Mongo for example
    Also, we should provide a better abstraction to isolate other code from specific
    schema used in our mongo tables etc.
    """

    WEBHOOKS_METADATA_TABLENAME = "webhooks"
    WEBHOOKS_SESSION_TABLE_NAME = "webhook_sessions"

    def __init__(self, webhook):
        self._webhook = webhook
        self._metadata = mongo_client()[METADATA_REPO_NAME][
            AirtableMetadata.WEBHOOKS_METADATA_TABLENAME
        ]
        self._sessions = mongo_client()[METADATA_REPO_NAME][
            AirtableMetadata.WEBHOOKS_SESSION_TABLE_NAME
        ]

    def load_local_state(self):
        return pd.DataFrame(self._metadata.find(self._active_webhook_predicate))

    @staticmethod
    def _time_labelled_hash():
        date_str = datetime.utcnow().strftime("%Y-%m-%d_%H:%M:%S")
        h = base64.b64encode(
            hashlib.md5(uuid.uuid1().hex.encode("utf-8")).digest()
        ).decode()
        return h + date_str

    @staticmethod
    def _hash_dict(d):
        if isinstance(d, str):
            d = literal_eval(d)
        # we support comparing on either { specification = {SPEC}} pr SPEC
        match_item = d["specification"] if "specification" in d else d
        return hashlib.sha1(
            json.dumps(match_item, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def _free_webhook_predicate(self, lock=None, timeout_minutes=5):
        decay_threshold = dates.utc_now() + timedelta(minutes=timeout_minutes)
        return {
            "$and": [
                {"_id": self._webhook._webhook_id},
                {
                    "$or": [
                        # the lock belongs to me (or is null)
                        {"lock": lock},
                        # or the lock is expired or invalid
                        {"lock_timestamp": {"$lt": decay_threshold}},
                        {"lock_timestamp": None},
                    ]
                },
            ]
        }

    @staticmethod
    def _table_id_from_webhook_id(webhook_id):
        metadata = mongo_client()[METADATA_REPO_NAME][
            AirtableMetadata.WEBHOOKS_METADATA_TABLENAME
        ]
        results = list(
            metadata.find(
                {
                    "id": webhook_id,
                }
            )
        )

        return None if len(results) == 0 else results[0]["table_id"]

    def invalidate(self):
        if self._webhook.webhook_id is not None:
            predicate = {"id": self._webhook._webhook_id}
            res = self._metadata.update_one(
                predicate,
                {"$set": {"invalidated_datetime": dates.utc_now().isoformat()}},
            )
            return res
        else:
            logger.info("There is no webhook linked to this session.")
            return None

    @property
    def _active_webhook_predicate(self):
        return {
            "table_id": self._webhook._table_id,
            "profile": self._webhook._profile,
            "deleted_timestamp": None,
        }

    def _get_unlocked_webhook(self, lock=None):
        """
        We can acquire a lock on anything that has no lock or is older than the timeout
        """
        # TODO read timeout from env / also maybe good to allow null timestamps to count as invalid locks

        res = pd.DataFrame(self._metadata.find(self._free_webhook_predicate())).to_dict(
            "records"
        )

        return None if len(res) == 0 else res[0]

    def add_session(self, webhook_id, profile, cursor, lock_key):
        session = self._session_object(webhook_id, profile, cursor, lock_key)
        logger.info(f"beginning streaming session {session['_id']}")
        self._sessions.insert(session)

    def close_session(self, webhook_id, profile, cursor, lock_key):
        my_session = self._get_unlocked_webhook(lock=lock_key)
        my_session["lock"] = None
        my_session["lock_timestamp"] = None
        # save my cursor for next time
        my_session["cursor"] = cursor
        metadata_update = self._metadata.update_one(
            {"_id": my_session["id"]}, {"$set": my_session}, upsert=True
        )

        session = self._session_object(webhook_id, profile, self._cursor, lock_key)

        session["ended"] = dates.utc_now()
        session["cursor_end"] = self._cursor
        session_update = self._sessions.update_one(
            {"_id": session["_id"]}, {"$set": session}, upsert=True
        )
        return (metadata_update, session_update)

    def _try_claim_session(self, key=None):
        key = key or AirtableMetadata._time_labelled_hash()
        logger.info(
            f"trying to acquire a lock on webhook {self._webhook._webhook_id} with key {key}"
        )
        unlocked_session = self._get_unlocked_webhook()
        if unlocked_session:
            logger.info(f"found that it is not locked, trying to claim with key {key}")
            unlocked_session["lock"] = key
            unlocked_session["lock_timestamp"] = dates.utc_now()
            # NB: to avoid race conditions we only update when we match a session without a valid lock!
            self._metadata.update_one(
                self._free_webhook_predicate(),
                {"$set": unlocked_session},
                upsert=True,
            )
            # the second time we (try) read it back it has to match the key we tried to create
            # otherwise someone else has the lock
            unlocked_session = self._get_unlocked_webhook(lock=key)
            if unlocked_session:
                self._cursor = (
                    unlocked_session["cursor"] if "cursor" in unlocked_session else 0
                )
                return unlocked_session["lock"]

            return None

        return None

    def _session_object(self, webhook_id, profile, cursor, key):
        # some session key that is basically unique
        client_key = profile + str(dates.utc_now())

        return {
            "client": client_key,
            "_id": hashlib.md5(
                f"{webhook_id}/{client_key}".encode("utf-8")
            ).hexdigest(),
            "webhook_id": webhook_id,
            "cursor_start": cursor,
            "created": dates.utc_now(),
            "cursor_end": None,
            "ended": None,
            # if there is no lock you can still create sessions but we can filter them
            "lock": key,
        }

    @staticmethod
    def match_type(field_type, metadata):
        fields = metadata[
            metadata["field_type_str"].map(
                lambda x: field_type in str(x) if not pd.isnull(x) else False
            )
        ]["ID"].values
        return fields

    @staticmethod
    def filter_type(field_type, metadata):
        fields = AirtableMetadata.match_type(field_type, metadata)

        def f(changes):
            return changes[changes["field_id"].map(lambda x: x in fields)]

        return f

    @staticmethod
    def make_issue_records(annotated_changes):
        issue_records = []
        for g, d in annotated_changes.groupby(["record_id", "timestamp"]):
            issue_set = d[d["issue_type"].notnull()]
            if len(issue_set) > 0:
                issue_set = dict(issue_set[["issue_type", "cell_value"]].values)
                issue_set.update({"record_id": g[0], "timestamp": g[1]})
                issue_records.append(issue_set)
        issue_records = pd.DataFrame(issue_records)
        logger.info(f"Issue records to add: {len(issue_records)}")

        return issue_records

    @staticmethod
    def annotate_changes(
        changes_in,
        metadata,
        keep_new=["node", "key", "is_issue_open", "issue_reason", "issue_tags"],
    ):
        in_cols = list(changes_in.columns)

        # ensure non lists
        def _clean(s):
            try:
                if isinstance(s, list):
                    s = ",".join(s)
                    return s
                return s
            except:
                return s

        # in particular we do not allow complex types
        changes_in["cell_value"] = changes_in["cell_value"].map(_clean)

        key_cols = metadata[metadata["is_key"]]["ID"].values
        kanban_fields = AirtableMetadata.match_type("Kanban", metadata)
        kanban_summary_fields = AirtableMetadata.match_type(
            "Kanban - Summary", metadata
        )
        issue_fields = AirtableMetadata.match_type("Issue", metadata)
        issue_type_map = dict(
            metadata[metadata["issue_type"].notnull()][["name", "issue_type"]].values
        )
        changes_in["is_key_field"] = changes_in["field_id"].map(lambda x: x in key_cols)
        changes_in["is_issue_field"] = changes_in["field_id"].map(
            lambda x: x in issue_fields
        )
        changes_in["is_kanban_field"] = changes_in["field_id"].map(
            lambda x: x in kanban_fields
        )
        changes_in["is_kanban_summary_field"] = changes_in["field_id"].map(
            lambda x: x in kanban_summary_fields
        )

        changes_in["issue_type"] = changes_in["column_name"].map(
            lambda x: issue_type_map.get(x)
        )

        try:
            sums = changes_in[
                ["is_kanban_field", "is_kanban_summary_field", "is_issue_field"]
            ].sum()
            if sums.sum() > 0:
                logger.info(f"Found special changed cells: {sums}")
        except Exception as ex:
            print("Temp: tried and failed to report on sums", repr(ex))

        keys = (
            changes_in[changes_in["is_key_field"]][
                ["record_id", "cell_value", "timestamp"]
            ]
            .drop_duplicates()
            .rename(columns={"cell_value": "key"})
        )

        # TODO the node mapping needs to be smarter. First we need to know what cell changed at this time and then
        # we need to map the correct node. if there is a leaf change we may not know which node it belongs to
        # we should add a hint in the metadata data
        nodes = (
            changes_in[changes_in["is_kanban_summary_field"]][
                ["record_id", "cell_value", "timestamp"]
            ]
            .drop_duplicates()
            .rename(columns={"cell_value": "node"})
        )

        changes_in = pd.merge(
            changes_in, nodes, on=["record_id", "timestamp"], how="left"
        )
        # the lky is required.
        changes_in = pd.merge(changes_in, keys, on=["record_id", "timestamp"])

        changes_in["node"] = changes_in["node"].fillna("-").astype(str)

        try:
            issue_records = AirtableMetadata.make_issue_records(changes_in)
            changes_in = pd.merge(
                changes_in, issue_records, on=["record_id", "timestamp"], how="left"
            )
            logger.info(
                f"Added res-issue records - there were {len(issue_records)} issue records"
            )

            for c in ["is_issue_open", "issue_reason", "issue_tags"]:
                if c not in changes_in:
                    changes_in[c] = None
            # set defaults
            changes_in["is_issue_open"] = (
                changes_in["is_issue_open"].fillna(False).astype(bool)
            )
            changes_in["issue_tags"] = changes_in["issue_tags"].fillna("").map(str)
            changes_in["issue_reason"] = changes_in["issue_reason"].fillna("").map(str)

        except Exception as ex:
            logger.info(
                f"unable to add issues to the payload or there are no issue fields: {repr(ex)}"
            )

        """
        [station]-issue-open   tag   {for each tag}   old-value [null] info {all tags} type kanban-issue
        [station]-issue-open   tag   {for each tag}   old-value [null] info {all tags} type kanban-issue
        [station]-issue-close  resolution_description  old-value [null] info {all tags} type kanban-issue
        """

        # this is some logic to try and make station status look like issues are part of them
        def _r(row):
            # the leaf detail is the one that gets the status change to append issue (not the node)
            if row["is_kanban_field"] and not row["is_kanban_summary_field"]:
                if row.get("is_issue_open", False):
                    return f"{row['cell_value']}-ISSUE"
            return row["cell_value"]

        # any leaf node that has issues will be modified into the issue state (which is what it should be)
        changes_in["cell_value"] = changes_in.apply(_r, axis=1)

        # preserve a schema that we care about
        return changes_in[[c for c in changes_in.columns if c in in_cols + keep_new]]


class _res_special_sets(object):
    @staticmethod
    def tables_we_move_to_snowflake(connector):
        pass

    @staticmethod
    def tables_that_have_nested_tables(connector):
        pass
