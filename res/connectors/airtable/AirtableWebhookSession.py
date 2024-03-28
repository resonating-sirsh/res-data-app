import res
from res.utils.configuration.ResConfigClient import ResConfigClient
import pandas as pd
from res.utils import logger


class AirtableWebhookSession(object):
    """
    Webhook session streams data from a specific webhook while also managing the cursor state
    """

    def __init__(
        self,
        base_id,
        table_id,
        webhook_id=None,
        annotate_key=False,
        use_latest_webhook=True,
    ):
        """
        Use the underlying airtable connector to get changes from a webhook given a webhook id
        Init the state for the particular table and webhook

        The latest webhook should not be used by default as we need to trust our cursor from the app
        However we can use in to fetch changes of any cursor ordinarily
        """
        logger.debug(f"Loading the airtable connector for {base_id}/{table_id}")
        self._connector = res.connectors.load("airtable")[base_id][table_id]
        self._key_column = None
        self._base_id = base_id
        self._table_id = table_id
        if annotate_key:
            fields = self._connector.fields
            self._key_column = fields[fields["is_key"]].iloc[0]["name"]

        self._config = ResConfigClient()
        self._cursor = 0

        # im not sure about this key - is there another way to add a composite key
        self._config_key = f"{self._connector.table_id}_webhook_config"
        self._session = None
        self._current_webhook_id = webhook_id

        if use_latest_webhook:
            logger.debug(f"Using latest webhook...")
            latest = AirtableWebhookSession.latest_webhook_for_table(
                base_id=base_id, table_id=table_id
            )
            if latest:
                self._current_webhook_id = latest[0]["id"]
                self._cursor = latest[0]["cursor"]
                logger.debug(
                    f"Using webhook {self._current_webhook_id} at cursor {self._cursor}"
                )

        if not self._current_webhook_id:
            self._restore()

    @property
    def cursor(self):
        return self._cursor

    @property
    def webhook_id(self):
        return self._current_webhook_id

    @staticmethod
    def latest_webhook_for_table(base_id, table_id):
        try:
            from res.connectors.airtable import AirtableWebhooks

            df = AirtableWebhooks.get_base_webhooks(base_id, table_id)
            return df.to_dict("records")
        except:
            return None

    def _restore(self):
        """
        restore should not be used normally as we should pass in a webhook id when responding to a ping on a particular webhook
        The use case where we do not pass the webhook id is a convenience for testing session construction without pings
        """
        # because the webhook id was not supplied we try to fetch it but normally it is best practice to specify one
        self._session = self._config.get_config_value(self._config_key)
        if self._session:
            self._cursor = self._session.get("cursor", 0)
            self._current_webhook_id = self._session.get("webhook_id")

    def __iter__(self):
        """
        iterate over the records in the change set and update the cursor
        returns chunks of dataframes but flatten for now just to provide another interface even thouhts its inefficient
        """
        if self._session:
            for change in self._connector._changes(
                self._current_webhook_id, self._cursor
            ):
                if self._key_column:
                    change = self._annotate(change)
                for record in change.to_dict("records"):
                    self._cursor = max(self._cursor, record["cursor"])
                    yield record
        else:
            raise NotImplementedError(
                "Calling the session iterator on a non-initialized session is not supported!"
            )

    def _annotate(self, df):
        """
        Add annotations to cell changes such as they key for that record
        We use the metadata to determine the key and gets its value
        This is updated for all cells by record id
        """
        df["base_id"] = self._base_id
        for key, data in df.groupby("record_id"):

            key_value = data[data["column_name"] == self._key_column].iloc[0][
                "cell_value"
            ]
            df.loc[df["record_id"] == key, "table_key"] = key_value
        return df

    @property
    def changes(self):
        """
        Get the entire change set in a dataframe
        Use the underlying iterator on self to update the cursor state
        """
        return pd.DataFrame(list(self))

    def _acquire_session_lock(self):
        """
        TODO: If we were worried about concurrency we could write/acquire a lock to the config here
        This also returns the session which is the approved lock but it would otherwise regturn Null
        By returning the default cursor value == 0 and the "current" webhook we are allowing for an empty config
        """
        return self._config.get_config_value(self._config_key) or {
            "webhook_id": self._current_webhook_id,
            "cursor": 0,
        }

    def __enter__(self):
        """
        Context manager enter's main job is to check that the webhook id we are being pinged on
        is the same as the one way know about:
        1. if it is we must retrive the cursor we are on
        2. otherwise we need to reset the cursor to 0

        In this implementation we always acquire "the session lock" but it is a placeholder to implement something smarter
        """
        self._session = self._acquire_session_lock()

        if self._session:
            self._cursor = self._session.get("cursor", 0)
            self._stored_webhook_id = self._session.get("webhook_id")

            # if the constructor's webhook id from airtable does not match what we have in config, reset to 0
            if self._stored_webhook_id != self._current_webhook_id:
                self._cursor = 0

        return self

    def __exit__(self, type, value, traceback):
        """
        Context manager exit's main job is to commit the cursor that we have read to
        If this were mission critical we would manage transactions better i.e. make write once assurances upstream.
        But we are sending non-true updates anyway so it is assumed the consumers know how to cope with that
        """
        self._config.set_config_value(
            self._config_key,
            {"cursor": self._cursor, "webhook_id": self._current_webhook_id},
        )


# Testing notes:
# 1.unit tests are difficult for this one but an integration test can use a test tables webhook and check that the cursor runs and consumes new cell changes
# 2.additionally we should test the case where the session runs after the webhook spec has changed
# 3.for extreme test we can check concurrency issues
