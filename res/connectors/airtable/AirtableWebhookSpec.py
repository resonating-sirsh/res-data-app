import json
from ast import literal_eval
import hashlib
from res.connectors.airtable.AirtableWebhooks import AirtableWebhooks
import res
from res.utils.configuration.ResConfigClient import ResConfigClient
from res.utils import logger
from typing import List
import os
from res.utils.secrets import secrets_client

# this is allways the same


class AirtableWebhookSpec(dict):
    """
    A wrapper for the airtable spec and a way to manage push and pull changes for comparison from different systems
    Handle things like
    - field ids vs field names when helping developers generate a spec
    - read and write specs from config
    - manage the notification url as a function of environment
    - sync ou config spec with the remote webhook api
    - check that the webhook is enabled for callbacks
    - when creating webhooks we enforce specifying adding a key - for the helpers for adding spec by names, we lookup the key field name and add it if it is not included in the user's field list

    An airtable spec [https://docs.google.com/document/d/104vs7OBczy1hbqV9mzVbHzuqarnw6w7yt_16BkuCnzc/edit] at table level looks like this

    {
      "specification": {
        {
            "scope": "tableRecords",
            "options": {
                "id": "tblXXX",
                "watchFields": ["fldYYY", ...] or "all",
                "reportFields": ["fldYYY", ...] or "all",
                "recordTransitionTypes": ["add", "remove", "none"] or "all",
              }
         }
      },
      "notificationUrl": "https://datadev.resmagic.io/airtable-webhooks/notify_airtable_cell_change/<profile>/<base>/<table>
    }

    We ignore the case where webhooks are added on entire bases

    """

    def __init__(self, *args, **kwargs):
        """
        Simply wrapping a dictionary with some semantics around what a spec is
        We need an external field mapping to manage equality properly - fetch from the connector
        """
        self._table_id = None
        self._base_id = kwargs.pop("base_id") if "base_id" in kwargs else None
        # get the field name from airtable - Name is the default airtable field
        # self._key_field_name = kwargs.pop("base_id") if "base_id" in kwargs else "Name"
        self.update(*args, **kwargs)
        self["base_id"] = self._base_id

    def _hash_dict(self, d):
        """
        utility for hasing and comparing a dict on relevant fields (we do not compare the notification url just in the spec in this object??)
        """
        if isinstance(d, str):
            d = literal_eval(d)
        # we support comparing on either { specification = {SPEC}} pr SPEC
        match_item = d["specification"] if "specification" in d else d
        return hashlib.sha1(
            json.dumps(match_item, sort_keys=True).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def config_as_schema(spec):
        """
        We keep the same information from an airtable perspective for the res scheam
        We can generate it here and the webhook listener has everything it needs
        """
        field_mapping = spec["field_mapping"]
        res_mapping = spec.get("res_schema_mapping", {})
        if res_mapping is None:
            res_mapping = {}
        res_mapping = {v: k for k, v in res_mapping.items()}

        # we map field id to airtable field name to res-schema name if different
        def resolve_field(f):
            airtable_f = field_mapping.get(f)
            return res_mapping.get(airtable_f, airtable_f)

        res_schema = {
            "airtable_table_id": spec["specification"]["options"]["id"],
            "airtable_base_id": spec["base_id"],
            "fields": [
                {
                    "key": resolve_field(f),
                    "name": resolve_field(f),
                    "is_key": f == spec.get("key_field"),
                    # TODO: for now :> we would need some sort of mapping
                    # maybe when we create the airtable spec we store here the mappings
                    # from airtabe field names to res names and then we build accordingly
                    "airtable_field_name": field_mapping.get(f),
                }
                for f in spec["specification"]["options"]["watchFields"]
            ],
        }

        return res_schema

    @staticmethod
    def _get_config_manager():
        return ResConfigClient(
            app_name="airtable-webhooks", namespace="res-infrastructure"
        )

    @staticmethod
    def try_get_res_schema_for_airtable(base_id, table_id, provider=None):
        """
        For now adding this mapping in code but in a git flow these would be added to some config
        (We would possibly dissociate from the webhook spec)
        """
        provider = provider or res.connectors.load("dgraph")

        mapping = {
            "workflow_template": "tblt9jo5pmdo0htu3",
            "meta.materials": "tblV45jYh1PsKuaNi",
            "meta.brands": "tblBdj7Xz4ITlTswg",
            "meta.bodies": "tblXXuR9kBZvbRqoU",
            "meta.styles": "tblmszDBvO1MvJrlJ",
            "meta.marker_requests": "tblwhgsCzGpt7odBg",
            "meta.body_requests": "tblrq1XfkbuElPh0l",
            "make.production_requests": "tblptyuWfGEUJWwKk",
            "make.print_assets": "tblwDQtDckvHKXO4w",
            "make.nest_assets": "tbl7n4mKIXpjMnJ3i",
            "make.test_rolls": "tblSYU8a71UgbQve4",
        }

        mapping = {v: k for k, v in mapping.items()}

        if mapping.get(table_id):
            return provider.try_load_meta_schema(mapping[table_id])

        logger.debug(f"There is no mapping to a res-schema for table {table_id}")
        return None

    def __eq__(self, other):
        if other is None:
            return False
        other_spec = other.get("specification", other)
        return self._hash_dict(self["specification"]) == self._hash_dict(
            other_spec or {}
        )

    def save_config(self):
        """
        Save the spec in config for a particular profile
        - in our config we can store just the table fields for each profile and we can optionally store the webhook id
        - the webhook id is something we can always ask airtable for but probably better to cache it
        - if we cache it we need to be sure that when we drop and recreate a webhook with a new spec, we update our id
        """
        key = f"{self._base_id}_{self.table_id}_webhook_spec"

        return AirtableWebhookSpec._get_config_manager().set_config_value(
            key, dict(self)
        )

    @property
    def watched_fields(self):
        connector = res.connectors.load("airtable")[self._base_id][self.table_id]
        metadata = dict(connector.fields[["id", "name"]].values)
        return {
            k: v
            for k, v in metadata.items()
            if k in self["specification"]["options"]["watchFields"]
        }

    @staticmethod
    def spec_from_res_schema(schema):
        """
        Using the metadata in the res-schema, generate a spec
        This will just pull out the base and table ids and then see which airtable fields we have in our schema
        """
        table_id = schema["airtable_table_id"]
        base_id = schema["airtable_base_id"]

        field_names = [
            f["airtable_field_name"]
            for f in schema["fields"]
            if f.get("airtable_field_name")
        ]

        return AirtableWebhookSpec.get_spec_for_field_names(
            base_id, table_id, field_names
        )

    @staticmethod
    def load_config(base_id, table_id):
        """
        For a particular environment, there can be one spec for each table in our scheme (we could support multiple profiles but dont)
        This method loads a spec from dynamo config for the env
        """
        key = f"{base_id}_{table_id}_webhook_spec"

        return AirtableWebhookSpec(
            AirtableWebhookSpec._get_config_manager().get_config_value(key),
            base_id=base_id,
        )

    @staticmethod
    def load_all():
        """
        Load all specs in the config
        """

        # example of loading for a static method - we may want to do this generally
        secrets_client.get_secret("AIRTABLE_API_KEY")

        def _base_from_key(key):
            return key.split("_")[0]

        return [
            AirtableWebhookSpec(spec, base_id=_base_from_key(key))
            for key, spec in AirtableWebhookSpec._get_config_manager()
            .get_config_entries()
            .items()
        ]

    @staticmethod
    def get_spec_for_field_ids(
        base_id, table_id: str, field_ids: List[str], key_field: str, **kwargs
    ):
        """
        Generate the spec object from the list of fields
        this is a developer tool to create the spec. see also spec from field names

        key field is required
        """
        spec = AirtableWebhookSpec._make_spec_for_field_ids(
            base_id, table_id, field_ids, key_field=key_field
        )

        return AirtableWebhookSpec(spec, base_id=base_id, key_field=key_field, **kwargs)

    @staticmethod
    def _make_spec_for_field_ids(base_id, table_id, field_ids, key_field):
        """
        Generate the spec structure from field ids for base/table
        Add the callback url basede on base/table and environment

        The key field is required
        """

        field_ids = sorted(list(set(field_ids + [key_field])))

        return {
            "specification": {
                "scope": "tableRecords",
                "options": {
                    "id": table_id,
                    "watchFields": field_ids,
                    "reportFields": field_ids,
                },
            },
            "notificationUrl": AirtableWebhookSpec.make_notification_url(
                base_id, table_id
            ),
        }

    @staticmethod
    def get_spec_for_field_names(
        base_id, table_id: str, field_names: List[str], **kwargs
    ):
        """
        This is a developer tool - given names for a base/table generate a spec using field ids
        See also spec from field ids

        We look up the field name which is required when creating webhooks by convention
        When using the by_names method we resolve the name but when using the lower level spec_for_field_ids the user must supply the field id as we do not want to do another lookup

        """
        connector = res.connectors.load("airtable")[base_id][table_id]
        fields = connector.get_extended_field_metadata()
        key_field = fields[fields["is_key"]].iloc[0]
        fields = dict(fields[["name", "id"]].values)
        if key_field["name"] not in field_names:
            field_names.append(key_field["name"])

        # this is a simple helper - we don't warn about bad names, just raise
        field_ids = [fields[name] for name in field_names]

        # field mappings are only for the fields we care about
        field_mapping = {v: k for k, v in fields.items() if v in field_ids}

        return AirtableWebhookSpec.get_spec_for_field_ids(
            base_id,
            table_id,
            field_ids,
            key_field=key_field["id"],
            field_mapping=field_mapping,
            **kwargs,
        )

    def make_notification_url(base_id, table_id):
        """
        There are many ways to configure this - try this for now
        """
        environment = os.environ.get("RES_ENV")
        # this could be added to the config map but lets see
        subdomain = "data" if environment == "production" else "datadev"
        return f"https://{subdomain}.resmagic.io/airtable-webhooks/default/{base_id}/{table_id}"

    @property
    def table_id(self):
        return self.table_options.get("id")

    @property
    def notification_url(self):
        """
        The notification url that we use e.g. /notify_airtable_cell_change/<profile>/<base>/<table>
        This can be generated from the environment
        """
        return self.get("notificationUrl")

    @property
    def table_options(self):
        return self.get("specification", {}).get("options", {})

    def merge(self, other_spec):
        """
        Compute relevant differences between a spec - and add any new fields to the spec for updating airtable
        we have to compare against either field id or names - airtable gives back field ids and we might be checkign against names - handle this here
        """
        pass

    def push(self):
        """
        Push spec changes to airtable (update api)
        - this will create a connection for base/table and update the webhook endpoint for the correct webhook id

        Returns the handle / webhook id and the spec
        """

        spec = {
            k: v for k, v in self.items() if k in ["specification", "notificationUrl"]
        }
        logger.debug(f"updating keys for spec {spec.keys()}")

        return AirtableWebhooks.update_or_fetch_webhook(
            self._base_id, self.table_id, spec
        )

    def status(self, as_dataframe=False):
        df = AirtableWebhooks.get_base_webhooks(self._base_id, self.table_id)
        return df if as_dataframe else df.to_dict("records")

    def pull(self):
        """
        Get the matching spec that is in airtabe i.e. for table and env (we should know our base/table/webhook_id for the request)
        - this could be used as part of a sync but really its just a sanity check to see what is currently registered with airtable
        - this will create a connection for base/table and update the webhook endpoint for the correct webhook id
        This does not necessarily belong here but i want to document the entire new flow in one place
        We need to know a base to get all webhooks but we can match for a table or a webhook id or a profile
        - e.g. get_webhooks(base_id, table_id=None, webhook_id=None, profile=None)

        Returns the id and the spec as a tuple.
        """

        # returns a dataframe of specs but in this context we only one there to be one record
        remote_specs = AirtableWebhooks.get_base_webhooks(
            self._base_id, table_id=self.table_id
        )

        # none for this table
        if remote_specs is None or len(remote_specs) == 0:
            return None, None

        # too many for this table
        if len(remote_specs) > 1:
            logger.error(
                f"There is more than one spec for table {self._table_id} which is not allowed"
            )
            raise Exception(
                "Too many webhooks - errors logged - TODO this should not happen at all"
            )

        remote_spec = remote_specs.iloc[0]

        return remote_spec["id"], remote_spec["specification"]

    def sync(self, ensure_enabled=True):
        """
        Some sort of workflow to make sure that what we have in config matches what is in airtable
        spec = AirtableWebhookSpec.load(table_id)
        spec.sync()

        By default we also check the webhook is enabled
        """
        wid, remote_spec = self.pull()
        if not self.__eq__(remote_spec):
            logger.debug("remote spec differs from self - pushing self")
            self.push()
        elif ensure_enabled:
            AirtableWebhooks.enable_callbacks_by_id(base_id=self._base_id, wid=wid)

    @staticmethod
    def sync_webhook(base_id, table_id):
        """
        We can load and sync a webhook
        This would be called by a cron job to ensure the health of webhooks
        """
        AirtableWebhookSpec.load_config(base_id, table_id).sync()
