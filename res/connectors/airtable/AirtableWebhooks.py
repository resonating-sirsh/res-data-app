from os import environ

from . import AIRTABLE_API_ROOT
from . import safe_http
from . import dates
from . import logger, get_airtable_request_header
import hashlib
import json
import pandas as pd
from ast import literal_eval

from .AirtableMetadata import AirtableMetadata

AIRTABLE_TIMEOUT = None

# TODO: Tests
# check bootstrapping for table or base that does not exist
#

UPSERT_WEBHOOK_SPECS = False

API_ROOT = environ.get("RES_API_BASE", "https://datadev.resmagic.io/res-connect")
# DEV/STAGING CALLBACK BASE URL
PING_URL_BASE = environ.get(
    "AIRTABLE_WEBHOOK_CALLBACK_URL",
    # "https://staging.api.resmagic.io/events/on_webhook_changed",
    f"{API_ROOT}/notify_airtable_cell_change",
)


class AirtableWebhooks(object):
    """
    Provides the wrapper around the webhooks api and implements some workflows
    for managing and updating webhooks based on our conventions
    We implement the api documented here https://docs.google.com/document/d/104vs7OBczy1hbqV9mzVbHzuqarnw6w7yt_16BkuCnzc/edit
    """

    RENAME_API_DATAFIELDS = {"perChangeHookTransactionNumberForNextPayload": "cursor"}

    def __init__(self, profile, table_id, base_id, check_owns=False):
        self._check_owns = check_owns
        self._profile = profile
        self._base_id = base_id
        self._table_id = table_id
        self._metadata_manager = AirtableMetadata(self)
        self._webhook_remote_state = AirtableWebhooks.get_base_webhooks(self._base_id)
        if (
            self._webhook_remote_state is not None
            and len(self._webhook_remote_state) > 0
        ):
            self._webhook_remote_state["table"] = self._webhook_remote_state[
                "specification"
            ].map(lambda x: x["options"]["id"])
        self._webhook_local_state = self._metadata_manager.load_local_state()

        self._api_url = f"{AIRTABLE_API_ROOT}/bases/{base_id}/webhooks"

        if len(self._webhook_local_state) > 1:
            logger.error(
                f"There must be no more than one active webhook for profile {self._profile} and table {self._table_id}"
            )

        self._webhook_id = (
            None
            if len(self._webhook_local_state) == 0
            else self._webhook_local_state.iloc[0]["id"]
        )

        self._cursor = (
            None
            if len(self._webhook_local_state) == 0
            else self._webhook_local_state.iloc[0]["cursor"]
        )

        self._locked_session_key = None

    def _repr_html_(self):
        try:
            rs = self._webhook_remote_state

            return rs.to_html()
        except Exception as ex:
            print("failed to load remote state", ex)
            return None

    @property
    def webhook_id(self):
        return self._webhook_id

    @property
    def our_cursor(self):
        return self._cursor

    @staticmethod
    def get_current_cursor(base_id, table_id):
        data = AirtableWebhooks.get_base_webhooks(base_id=base_id, table_id=table_id)
        if len(data) == 0:
            return 0
        if len(data) == 1:
            return data.iloc[0]["cursor"]
        raise Exception(
            f"There should be zero or one webhooks for any given table. Found {len(data)} of {base_id}/{table_id}"
        )

    @staticmethod
    def get_base_webhooks(base_id, table_id=None) -> pd.DataFrame:
        """
        webhooks are requested at base level
        we can also filter by table id
        """
        url = f"{AIRTABLE_API_ROOT}/bases/{base_id}/webhooks"

        json_data = safe_http.request_get(
            url, headers=get_airtable_request_header(), timeout=AIRTABLE_TIMEOUT
        ).json()
        if not "changeHooks" in json_data:
            return None

        data = pd.DataFrame([r for r in json_data["changeHooks"]])
        if len(data) == 0:
            return None

        data = data.rename(columns=AirtableWebhooks.RENAME_API_DATAFIELDS)

        if len(data) > 0:
            data["table_id"] = data["specification"].map(lambda x: x["options"]["id"])
            if table_id:
                data = data[data["table_id"] == table_id].reset_index().drop("index", 1)

        return data

    @staticmethod
    def enable_callbacks_by_id(base_id, wid):
        api_root = f"{AIRTABLE_API_ROOT}/bases/{base_id}/webhooks"

        safe_http.request_post(
            f"{api_root}/{wid}/enableNotifications",
            headers=get_airtable_request_header(),
            json={"enable": True},
            timeout=AIRTABLE_TIMEOUT,
        )

    def delete_base_webhook(base_id, webhook_id):
        api_root = f"{AIRTABLE_API_ROOT}/bases/{base_id}/webhooks"
        if webhook_id is not None:
            response = safe_http.request_delete(
                f"{api_root}/{webhook_id}",
                headers=get_airtable_request_header(),
                timeout=AIRTABLE_TIMEOUT,
            )

            # check response

            logger.info(f"Webhook {webhook_id} deleted")

    ########## WORKFLOW ###########

    def invalidate(self):
        res = self._metadata_manager.invalidate()
        # check this metadata if its none we need to create a webhook because there is none
        # if the webhooks are disabled we need to re-enable
        # if the webhook for some reason has no callback we need to re-create

    def diff_frame(self):
        common_cols = ["id", "cursor", "notificationUrl", "specification"]
        ours = self._webhook_local_state.reset_index()[common_cols]
        theirs = self._webhook_remote_state.reset_index()[common_cols]
        ours["spec_hash"] = ours["specification"].map(AirtableMetadata._hash_dict)
        theirs["spec_hash"] = theirs["specification"].map(AirtableMetadata._hash_dict)

        ours = ours.unstack().reset_index()
        ours["which"] = "ours"
        theirs = theirs.unstack().reset_index()
        theirs["which"] = "theirs"

        diff = pd.concat([ours, theirs])
        diff = diff.rename(columns={"level_0": "-"}).pivot("which", "-", 0).T
        diff["has_diffs"] = diff.apply(
            lambda row: str(row["ours"]) != str(row["theirs"]), axis=1
        )

        return diff

    def __enter__(self):

        """
        contextmanager function - this is read_write mode to acquire a lock on as specific webhook
        if we dont do this we should not be allowed to do some things
        like update streams or write to mongo sessions

        NB: when testing this, the context manager will remove session data when you exit the context
        with OBJECT as session:
            pass #do somethings
        # the session lock and timestamp will be removed!
        """
        assert (
            self._webhook_id is not None
        ), "To create a context session you must specify a webhook id"

        assert (
            self._profile is not None
        ), "To create a context session you must specify a profile key"

        self._locked_session_key = self._metadata_manager._try_claim_session()

        if self._locked_session_key:
            self._metadata_manager.add_session(
                self._webhook_id,
                self._profile,
                int(self._cursor),
                self._locked_session_key,
            )

        return self

    def __exit__(self, type, value, traceback):
        if self._locked_session_key:
            self._metadata_manager.close_session(
                self._webhook_id,
                self._profile,
                int(self._cursor),
                self._locked_session_key,
            )
            logger.info(
                f"webhook session {self._locked_session_key} closed happily for webhook {self._webhook_id}. saving cursor location {self._cursor}"
            )

    def refresh_invalid_webhook_from_metadata(
        self, metadata, create_if_not_exists=False
    ):
        """
        Any webhook in our database with an invalidate flag - will be rebuilt from metadata
        """
        # normally this will be called in a session where we have just read from a webhook
        # but we can also use it in other contexts (mostly for testing)
        # TODO: refactor

        logger.info(
            f"refreshing webhook with id {self._webhook_id} in profile {self._profile}"
        )

        invalid_webhooks = list(
            self._metadata_manager._metadata.find(
                {
                    "id": self._webhook_id,
                    "profile": self._profile,
                    "invalidated_datetime": {"$ne": None},
                }
            )
        )

        if len(invalid_webhooks) > 0:
            logger.info("updating invalidated webhook")
            existing_spec = invalid_webhooks[0]["specification"]
            # pass the saved webhook (which could be made up) and merged with whatever the new metadata says
            self._update_webhook_from_metadata(
                self._table_id, metadata, saved_webhook=existing_spec
            )
        else:
            res = self._metadata_manager._metadata.find(
                {
                    "id": self._webhook_id,
                    "profile": self._profile,
                }
            )
            if create_if_not_exists and res.count() == 0:
                logger.info(
                    "We did not find a matching webhook creating new as the create new option is set...."
                )
                self._update_webhook_from_metadata(self._table_id, metadata)
            else:
                logger.info("We did not find a matching webhook so no action taken.")

        return invalid_webhooks

    def _update_webhook_from_metadata(
        self,
        table_id,
        metadata,
        saved_webhook=None,
    ):
        """
        Load the metadata from the fields table and register the webhook using the columns specified in the metadata
        """

        # check what fields need a webhook for this profile webhook_[PROFILE_KEY]_profile
        # TODO in future if we have multiple profiles this will need to change
        metadata = metadata[
            (metadata["is_valid"])
            & ((metadata["webhook_default_profile"]) | (metadata["is_key"]))
        ]

        # we can add add watch with either names or ids but better to use ids as Airtable returns these and we need to diff
        # NOTE for our test webhook this will be empty as we spoof add it and it is not in our FIELDS metadata
        watched_columns = list(metadata["id"])

        if saved_webhook is not None:
            existing = saved_webhook["options"]["watchFields"]
            required_to_add = list(set(watched_columns) - set(existing))
            required_to_drop = list(set(existing) - set(watched_columns))

            logger.info(
                f"there are {len(required_to_add)} columns to add for this table's webhook and {len(required_to_drop)} to drop"
            )

            # TODO: we could just remain "additive" and do not drop cols that we do not need to watch
            # this has the advantage of preserving the webhook while keeping cols that we no longer need
            # it would be cleaner though, to always be in sync with our metadata and then push changes
            # we could preserve existing VALID but for now do not... compute valid_existing
            # watched_columns = list(set(watched_columns) | set(existing))

        # only if there is something to do
        if len(watched_columns) != 0:
            # confirm that these are all valid i.e. 'existing' could be deleted from the table
            spec = {
                "scope": "tableRecords",
                "options": {
                    "id": table_id,
                    "watchFields": watched_columns,
                    "reportFields": watched_columns,
                },
            }
            spec = {"specification": spec}

            self.get_or_create_webhook(spec)

    def _get_default_table_spec(self):
        spec = {
            "scope": "tableRecords",
            "options": {
                "id": self._table_id,
                "watchFields": "all",
                "reportFields": "all",
            },
        }
        return {"specification": spec}

    def get_or_create_webhook(self, spec=None, mode="create"):
        """
        We do not allow get without a spec unless spec is None but really at table level this should not happen
        In general we will be matching the spec /creating it or updating it

        TODO: generally when creating and deleting webhooks we do not have a string
              transaction with what is no out mongo meta store which should be in sync with airtable
              this is very much a test situation - in future we should abstract out the
              webhook repository properly so that (a) these are robustly synced and (b) we can swop our metastore
        """

        if spec is None:
            spec = self._get_default_table_spec()

        def hash_dict(d):
            if isinstance(d, str):
                d = literal_eval(d)
            # we support comparing on either { specification = {SPEC}} pr SPEC
            match_item = d["specification"] if "specification" in d else d
            return hashlib.sha1(
                json.dumps(match_item, sort_keys=True).encode("utf-8")
            ).hexdigest()

        def get_table(d):
            if isinstance(d, str):
                d = literal_eval(d)
            # TODO: we are not supporting bases scope only table scope
            return d["specification"]["options"]["id"]

        table_id = get_table(spec)
        # this key is important because we can only have jurisdiction over url matches
        # TODO: integration test - create webhook and make sure that ping gets pack via server
        # WE
        client_key = f"{PING_URL_BASE}/{self._profile}/{self._base_id}"
        if table_id is not None:
            client_key += f"/{table_id}"
        # this is how we will be pinged
        logger.info(f"checking webhooks for the key {client_key}")

        existing = self.get_base_webhooks(self._base_id)
        if existing is not None and len(existing) > 0:
            existing = existing[existing["notificationUrl"] == client_key]

            if mode == "update" and len(existing) > 0:
                # this updates in the sense that it merges to the existing fields but still deletes webhook
                table_match = existing[
                    existing["specification"].map(get_table) == table_id
                ]
                if len(table_match) > 0:
                    spec.update(table_match.iloc[0]["specification"])

            # if we already have an identical spec or there is a blank spec just return what we have
            matching = (
                existing[existing["specification"].map(hash_dict) == hash_dict(spec)]
                if spec is not None
                else existing
            )
            if len(matching) > 0:
                row = dict(matching.iloc[0])
                logger.info(
                    f"existing webhook {row['id']} matched, ensuring pings are enabled"
                )
                self.enable_callbacks()
                return row

            # we need to purge similar ones that belong to our profile
            for _, v in existing.iterrows():
                self._delete_webhook(v["id"])

        spec["notificationUrl"] = client_key
        # recreate with the new spec

        logger.info(f"requesting webhook {spec}")

        response = safe_http.request_post(
            self._api_url,
            headers=get_airtable_request_header(),
            json=spec,
            timeout=AIRTABLE_TIMEOUT,
        ).json()

        # save and return / reload with metadata
        if "id" in response:
            return self._pull(response)

        return None

    @staticmethod
    def update_or_fetch_webhook(base_id, table_id, spec):
        """
        Three cases
        1. There is no existing spec, we just create it and return the latest
        2. There is an existing spec that is different to the one we want so we must
          a. deleted the existing by id
          b. create the new one
        3. There is an existing spec but its the same, just return the id and spec

        If there are multiple specs already for base/table this is an invalid state
        """

        api_url = f"{AIRTABLE_API_ROOT}/bases/{base_id}/webhooks"

        existing_spec = AirtableWebhooks.get_base_webhooks(
            base_id=base_id, table_id=table_id
        )

        if existing_spec is not None and len(existing_spec) > 1:
            raise Exception(f"There is more than one webhook for {base_id}/{table_id}")

        no_existing_spec = existing_spec is None or len(existing_spec) == 0

        if no_existing_spec or existing_spec.iloc[0]["specification"] != spec:
            # if there is an existing but it does not match we should delete it
            if not no_existing_spec:

                AirtableWebhooks.delete_base_webhook(
                    base_id, existing_spec.iloc[0]["id"]
                )
            # now create a new one
            response = safe_http.request_post(
                api_url,
                headers=get_airtable_request_header(),
                json=spec,
                timeout=AIRTABLE_TIMEOUT,
            ).json()
            # verify
            if "id" in response:
                return response["id"], spec

            # the one we fetched is fine as there are no changes
            return existing_spec["id"], existing_spec["specification"]

    def enable_callbacks(self):
        safe_http.request_post(
            f"{self._api_url}/{self._webhook_id}/enableNotifications",
            headers=get_airtable_request_header(),
            json={"enable": True},
            timeout=AIRTABLE_TIMEOUT,
        )

    def _pull(self, response):
        self._webhook_remote_state = AirtableWebhooks.get_base_webhooks(self._base_id)

        wid = response["id"]
        logger.info(f"Webhook {wid} created")
        # load and update the metadata

        hooks = self._webhook_remote_state[self._webhook_remote_state["id"] == wid]
        row = hooks.to_dict("records")[0]
        row["table_id"] = row["specification"]["options"]["id"]
        row["profile"] = self._profile
        row["invalidated"] = False
        row["invalidated_datetime"] = None
        row["cursor"] = 0
        row["lock"] = None
        row["lock_timestamp"] = None
        row["created"] = dates.utc_now().isoformat()
        self._metadata_manager._metadata.update_one(
            {"_id": row["id"]}, {"$set": row}, upsert=True
        )
        return row

    def _delete_webhook(self, webhook_id):

        if webhook_id is not None:
            safe_http.request_delete(
                f"{self._api_url}/{webhook_id}",
                headers=get_airtable_request_header(),
                timeout=AIRTABLE_TIMEOUT,
            )

            self._metadata_manager._metadata.update_one(
                {"id": webhook_id},
                {"$set": {"deleted_timestamp": dates.utc_now().isoformat()}},
            )

            logger.info(f"Webhook {webhook_id} deleted")

    def push_spec_changes(self, spec, delete_existing_but_different=True):
        """
        Use the existing methods for a new flow to push changes to the webhook endpoint for profile/table
        return the new or existing webhook id

        Note, if the spec is different there is a bit more to the workflow as we should also delete an old match
        """
        url = None  # resolve_for_spec(spec) # the url is determined by the base/table as provided in the spec
        response = safe_http.request_post(
            url,
            headers=get_airtable_request_header(),
            json=spec,
            timeout=AIRTABLE_TIMEOUT,
        ).json()

        # TODO

    @staticmethod
    def health():
        def get_table(s):
            return s["options"]["id"]

        def get_base(x):
            for b in str(x).split("/"):
                if len(b) > 3 and b[:3] == "app":
                    return b
            return None

        def parser(s):
            def _f(d):
                if d is None:
                    return None
                return d[s]

            return _f

        from res.connectors.mongo import MongoConnector

        allhooks = pd.DataFrame(MongoConnector()["test"]["webhooks"].find())
        ah = allhooks[
            [
                "_id",
                "areNotificationsEnabled",
                "current_cursor",
                "cursor",
                "lastNotificationResult",
                "lastSuccessfulNotificationTime",
                "notificationUrl",
                "profile",
                "table_id",
                "deleted_timestamp",
                "invalidated",
            ]
        ]
        ah = ah[ah["deleted_timestamp"].isnull()]
        ah["base"] = ah["notificationUrl"].map(get_base)
        ah.sort_values("cursor")

        wdata = []
        for b in ah["base"].unique():
            print(b)
            if b:
                df = AirtableWebhooks.get_base_webhooks(b)
                if df is not None:
                    df["base_id"] = b
                    wdata.append(df)
        wdata = pd.concat(wdata)
        wdata["table_id"] = wdata["specification"].map(get_table)
        wdata = wdata.reset_index()
        for key in ["success", "completionTimestamp", "durationMs", "retryNumber"]:
            f = parser(key)
            wdata[key] = wdata["lastNotificationResult"].map(f)

        logger.info("Checking for stale webhooks and renabling them")
        for _, r in wdata[wdata["areNotificationsEnabled"] == False].iterrows():
            base, wid = r["base_id"], r["id"]
            print("renable webhooks for base", base, wid)
            AirtableWebhooks.enable_callbacks_by_id(base, wid)

        return wdata[
            [
                "id",
                "table_id",
                "base_id",
                "success",
                "completionTimestamp",
                "durationMs",
                "areNotificationsEnabled",
            ]
        ].sort_values("success")
