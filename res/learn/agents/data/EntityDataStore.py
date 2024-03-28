from langchain.agents import Tool
import res
from langchain.tools import Tool
from schemas.pydantic.common import FlowApiAgentMemoryModel
from typing import List
from tqdm import tqdm


def EntityDataStoreAny():
    """
    entity agnostic interface
    """
    return EntityDataStore(None, None)


class EntityDataStore:
    ENTITY_CONTEXT = {
        "style": "To answer questions about the body, material and color for this style note that the sku identifier is made up three parts; Body, Material and Color"
    }

    def __init__(
        self, namespace, entity_name, store_name="ONE_STORE", try_and_add_fn=None
    ):
        self._entity_name = entity_name
        self._namespace = namespace
        self._db = res.connectors.load("redis")["RRM"][store_name]
        self._try_and_add_fn = try_and_add_fn

    @property
    def entity_name(self):
        return self._entity_name

    def insert_row(self, d, key, etype=None, **options):
        # extract they key
        key = d[key]
        d[".entity_type"] = etype
        self._db.put(key, d)
        return d

    def upsert_row(self, d, key, etype=None, **options):
        # extract they key
        key = d[key]
        d_old = self._db[key] or {}
        d_old.update(d)
        self.insert_row(d, d_old, etype=etype)
        return d

    def add(self, records: List[FlowApiAgentMemoryModel]):
        assert (
            self._entity_name
        ), "You cannot use the add function without specifying the entity name and namespace in the constructor"
        records = [r.attribute_dict() for r in records]
        records = [r for r in records if len(r)]

        for record in tqdm(records):
            self.insert_row(record, key="id", etype=self._entity_name)

    def __getitem__(self, key):
        added = None
        if self._try_and_add_fn:
            key, added = self._try_and_add_fn(key)
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
        return value

    def as_tool(self):
        return entity_resolver_tool_from_store(self)


def entity_resolver_tool_from_store(
    store: EntityDataStore,
    help_text="use the stats tool to answer questions of a statistical nature or the further details too for more arbitrary questions",
):
    """
    We build entities from a dataframe
    note we always provide a help hint pointer to another tool
    """

    # override test specificity
    entity_name = store.entity_name
    if entity_name == "one":
        entity_name = "production requests"

    help_text = (
        f"use the stats tool for entities of type {entity_name} to answer questions of a statistical nature or the further details tool for entities of type {entity_name} for more arbitrary questions",
    )

    def fetcher(keys):
        keys = keys.split(",")
        # do some key cleansing
        # TODO figure out what the regex is to do this safely for identifiers - this is a crude way as we experience cases
        # Tests for new lines and quotes etc in the thing or stuff at the ends that is not simple chars
        keys = [k.rstrip("\n").lstrip().rstrip().strip('"').strip("'") for k in keys]
        # the store may internal try and add something to the map
        # for example if we pass in A B C D it may lookup A B C and add D in the result but map context from ABC
        d = {k: store[k] for k in keys}
        d["help"] = help_text
        return d

    return Tool(
        name="Entity Resolution Tool",
        func=fetcher,
        description="""use this tool when you need to lookup entity attributes or find out more about some code or identifier.
            Do not use this tool to answer questions of a statistical nature.
            You should pass a comma separated list of known or suspected entities to use this tool""",
    )
