from typing import Any
import res
from res.observability.io import AbstractStore
import typing
from tqdm import tqdm
from res.observability.entity import AbstractEntity
from enum import Enum


def EntityDataStoreAny():
    """
    entity agnostic interface
    """
    return EntityDataStore(None, None)


class Caches(Enum):
    DEFAULT = "OBVS_ENTITIES"
    QUEUES = "QUEUE_ENTITIES"


def hack_variant(s):
    """
    for resonance orders, body codes and skus provide a normed vs + a SKU to BMC variant
    e.g.
    JR6789 -> JR-6789
    JR6789 ABCDE ADFGD 2ZZSM -> JR-6789 ABCDE ADFGD

    """
    import re

    def hyphenate_code(text):
        pattern = r"([A-Za-z]{2})(\d{4,})"
        replaced_text = re.sub(pattern, r"\1-\2", text)
        return replaced_text

    ns = hyphenate_code(s)

    if len(ns.split(" ")) == 4:
        return f" ".join(ns.split(" ")[:3])

    if "-PROD" in ns:
        # that annoying -PROD on the MONE keys - why is there anyway
        return ns.replace("-PROD", "")

    if ns != s:
        return ns


class EntityDataStore(AbstractStore):
    ENTITY_CONTEXT = {
        "style": "To answer questions about the body, material and color for this style note that the sku identifier is made up three parts; Body, Material and Color"
    }

    def __init__(
        self,
        entity: AbstractEntity = AbstractEntity,
        alias: str = None,
        description: str = None,
        # the cache name can be something else e.g. focusing on queues
        cache_name: Caches = Caches.QUEUES,
    ):
        super().__init__(entity=entity, alias=alias, description=description)
        self._cache_name = cache_name

        self._db = res.connectors.load("redis")["OBSERVE"][cache_name]

    @property
    def entity_name(self):
        return self._entity_name

    def insert_row(self, d, key_field, etype=None, **options):
        # extract they key
        if hasattr(d, "dict"):
            d = d.dict()
        key = d[key_field]
        d[".entity_type"] = etype
        # this is so we can have multiple mappings to the same key for disambiguation
        existing = self._db[key] or {}
        if options.get("mode") == "overwrite":
            existing = {}
        existing[etype] = d
        if len(existing) > 1:
            d[
                "hint"
            ] = f"There are multiple entities with the same name key {key} so you should infer the appropriate type"
        self._db.put(key, existing)
        return d

    def upsert_row(self, d, key, etype=None, **options):
        if hasattr(d, "dict"):
            d = d.dict()
        # extract they key
        key = d[key]
        d_old = self._db[key] or {}
        d_old.update(d)
        self.insert_row(d, d_old, etype=etype)
        return d

    def add(
        self,
        records: typing.List[typing.Union[AbstractEntity, dict]],
        key_field="name",
        mode=None,
    ):
        """
        Loads entities as dicts or types. A useful way to get typed entities is to read them from another store eg.

        store = ColumnarDataStore.open(name='style', namespace='meta')
        Model = store.entity
        estore = EntityDataStore(store.entity)
        estore.add(<records>)

        You can read entities generically with
        store = EntityDataStore(AbstractEntity)
        store[KEY]

        This will return a typed map or a map of maps for each entity
        a map of maps is sued simply to accommodate key collisions over types

        """
        assert (
            self._entity_name
        ), "You cannot use the add function without specifying the entity name and namespace in the constructor"

        if not isinstance(records, list):
            records = [records]

        def get_r(d):
            # support pydantic or dict
            if hasattr(d, "dict"):
                return d.dict()
            return d

        records = [get_r(r) for r in records]
        records = [r for r in records if len(r)]

        if key_field is None:
            key_field = self._key_field

        for record in tqdm(records):
            self.insert_row(
                record,
                key_field=key_field,
                etype=f"{self._entity_namespace}.{self._entity_name}",
                mode=mode,
            )

    def __getitem__(self, key):
        added = None

        value = self._db[key]

        if value:
            etype = value.get(".entity_type")
            if added:
                if etype:
                    ac = EntityDataStore.ENTITY_CONTEXT.get(etype.lower())
                    if ac:
                        added = added + ac
                value[".context"] = added
            # principle of no dead ends
            # value["help"] = "use the stats tool to find out more"

        else:
            return "There was no record matching this key - please try another store"
        return value

    def run_search(self, keys: typing.List[str]):
        """
        use this function when you need to lookup entity attributes or find out more about some codes, skus, names or identifiers
        Entities are anything that looks like name or identifiers especially Orders, ONEs, SKus, Style Codes, Bodies, Materials and Colors
        Do not use this tool to answer questions of a statistical nature
        You should pass a comma separated list or a string list of known or suspected entities

        **Args**
            keys: a command separated list of keys or a collection of keys. By default upper casing is usually used for such keys!
        """

        help_text = f"""If you have a function specifically related to this entity use the ColumnarDataStore functions or if you do not find results with that you could try various vector store searches
            for arbitrary longer form questions about entities."""

        keys = keys.split(",") if not isinstance(keys, list) else keys
        # do some key cleansing
        # TODO figure out what the regex is to do this safely for identifiers - this is a crude way as we experience cases
        # Tests for new lines and quotes etc in the thing or stuff at the ends that is not simple chars
        keys = [k.rstrip("\n").lstrip().rstrip().strip('"').strip("'") for k in keys]
        # the store may internal try and add something to the map
        # for example if we pass in A B C D it may lookup A B C and add D in the result but map context from ABC
        # note we omit adding null responses!!

        try:
            # trick to add variants to support alternate key formats for orders, bodies and skus
            variants = [hack_variant(s) for s in keys]
            variants = [s for s in variants if s is not None]
            keys += variants
        except:
            pass

        d = {k: self[k] for k in keys if self[k]}
        d["help"] = help_text
        return d

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.run_search(*args, **kwargs)
