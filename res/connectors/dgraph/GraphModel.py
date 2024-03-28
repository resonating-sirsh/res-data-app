EXAMPLE_SPEC = """
topics:
  - name: res_meta.dxa.prep_pieces_responses
    schema_path: res-meta/dxa/prep_pieces_responses.avsc.untracked
    #when there are multiples - we add a schema key to pick one
    schema_key: 
    alias: print_assets
    key: id
    relationships:
      - name: body_code
        entity_type: bodies
        cardinality: 1
      - name: material_code
        entity_type: materials
      - name: color_code
        entity_type: colors
      - name: one_number
        entity_type: make_orders
    airtable:
      development: appUFmlaynyTcQ5wm/tblOJwkggChGdJ91C
      production:
      additional_two_way_bindings:
    interfaces: []
    qualified_fields: []
    type_extensions: []

"""
import yaml
import json
import res
from pathlib import Path
from res.utils import get_res_root
from stringcase import titlecase
from datetime import datetime
from res.utils.schema import python_type_from_avro_type
from res.connectors.airtable.AirtableConnector import AIRTABLE_ONE_PLATFORM_BASE


RES_HOME = get_res_root()
RES_SCHEMA_HOME = f"{RES_HOME}/res-schemas"
AVRO_SCHEMA_HOME = f"{RES_SCHEMA_HOME}/avro"
# def generate_gql_from_topics_graph(path):
#     pass


class TopicModel(dict):
    def __init__(self, config, avro_schema, graph, **options):
        """

        Supply a dict avro type - at this point the avro schema must specify a type e.g. some avro has multiple types but the loader should pick one to pass here
        """
        self._topic_key = config.get("schema_key")
        path = config.get("schema_path")
        self._name = self._topic_key
        self._shared_aliases = {}
        res.utils.logger.debug(
            f"Adding topic {self._name}({config['alias']}) from {path}"
        )
        if not isinstance(avro_schema, dict):
            raise Exception("The avro schema should be a dictionary")
            # convenience where we merge the avro and custom schema - this is a little risky but we can test it
        if (
            graph is not None and self._topic_key is not None
        ):  # for uunit testing topic we can add null graph
            res.utils.logger.debug(
                f"Adding alias {self._topic_key}->{config['alias']} to graph"
            )
            graph._shared_aliases[self._topic_key] = config["alias"]
            self._shared_aliases = graph._shared_aliases
        else:
            res.utils.logger.debug(
                f"No graph or topic key passed to topic model constructor"
            )

        self._graph = graph
        full = dict(avro_schema)
        full.update(config)
        # this works if we get internal props self[self._name]['prop']
        full["referenced_types"] = []
        # we elevate this to be something that can be merge for multiple types
        full = {self._name: full}
        super(TopicModel, self).__init__(full)

        self._config = config
        self._alias = config["alias"]
        self._topic_name = config["name"]
        self._avro_config = avro_schema

        self._namespace = config.get("namespace")
        self._key_field = config.get("key")
        self._field_names = [f["name"] for f in avro_schema["fields"]]
        self._options = options
        # this could be refactored but this is our compiler
        # we write back some info to the avro type here
        self._relationships = self._config.get("relationships", [])
        self._relationships = {r["name"]: r for r in self._relationships}

        # airtable type info
        self._airtable = config.get("airtable", {})
        self._airtable_key = self._airtable.get("key_map_name")

    def init(self):
        try:
            self._predicates = self._make_predicates_dql()
        except Exception as ex:
            res.utils.logger.warn(f"Unable to generate predicates {repr(ex)}")
            raise ex

    @property
    def field_names(self):
        return self._field_names

    @property
    def type_dql(self):
        NL = "\n"
        fields = self._field_names
        edges_for_fields = self._graph.edges_for_fields(fields)
        fields = edges_for_fields + fields
        return f"""type {self._alias}{{{NL}{NL.join(fields)}{NL}}}"""

    def _map_primitive(self, name, ftype, pluralize=False):
        if not isinstance(ftype, list):
            ftype = [ftype]
        if "string" in ftype:
            return f"<{name}>: string ." if not pluralize else f"<{name}>: [string] ."
        elif "float" in ftype:
            return f"<{name}>: float ." if not pluralize else f"<{name}>: [float] ."
        elif "int" in ftype:
            return f"<{name}>: int ." if not pluralize else f"<{name}>: [int] ."
        elif "boolean" in ftype:
            return f"<{name}>: bool ." if not pluralize else f"<{name}>: [bool] ."
        else:
            raise Exception(f"Avro primitive type {ftype} not yet supported")

    def is_known_complex_avro_type(self, t):
        if t in self._shared_aliases:
            return True
        return False

    @property
    def predicates_dql(self):
        return self._predicates

    @property
    def primitive_fields(self):
        primitives = []
        for f in self._avro_config["fields"]:
            ftype = f["type"]
            if f["name"] == self._key_field:
                f["is_key"] = True
            # assign precedence
            if isinstance(ftype, list) or isinstance(ftype, str):
                primitives.append(f)
            elif isinstance(ftype, dict):
                if ftype.get("type") == "array":
                    item_type = ftype.get("items", "")
                    item_type = item_type.split(".")[-1]
                    ftype["items"] = item_type
                    if not self.is_known_complex_avro_type(item_type):
                        primitives.append(f)
        return primitives

    @property
    def airtable_attachments(self):
        return self._airtable.get("attachments", [])

    def attachment_iterator_for_asset(self, asset, case_function=None):
        """
        this could maybe go somewhere else but based on the config we can parse an object and pluck out its attachments
        we create an iterated map over pieces because then in update airtable, airtable can replace any attachment fields with the generator
        the generator is good because we sign the url last minute

        this requires some testing because we do not know all the ways attachments will be linked to objects
        - serialized json map
        - json map
        - list of records (this one is the one that requires a property to be selected on the records)
        """

        s3 = res.connectors.load("s3")

        def itit(k):
            if asset:
                ov = asset[k] if not case_function else asset[case_function(k)]
                # the string is a json object - we parse it
                if isinstance(ov, str):
                    try:
                        ov = json.loads(ov)
                    except Exception as ex:
                        # try it as a simple path
                        if s3.exists(ov):
                            yield s3.generate_presigned_url(ov)
                        else:
                            res.utils.logger.warn(
                                f"Cannot parse the json for this attachment {ov} - skipping in asset {asset}"
                            )
                # we can then just map
                if isinstance(ov, dict):
                    for k, v in ov.items():
                        yield s3.generate_presigned_url(v)

                # or we can have types with a path
                if isinstance(ov, list):
                    for item in ov:
                        s = item.get(rec["path"])
                        if s:
                            res.utils.logger.debug(s)
                            yield s3.generate_presigned_url(s)

        iterators = {}
        for rec in self.airtable_attachments:
            k = rec["name"]
            iterators[k] = itit(k)

        return iterators

    def _make_predicates_dql(self):
        types = {}
        for f in self._avro_config["fields"]:
            ftype = f["type"]
            # assign precedence
            if isinstance(ftype, list) or isinstance(ftype, str):
                types[f["name"]] = self._map_primitive(f["name"], ftype)
                rel = self._relationships.get(f["name"])
                if rel:
                    f["type_alias"] = rel["entity_type"]
            if isinstance(ftype, dict):
                # {{'type': 'array', 'items': 'string'}}
                if ftype.get("type") == "array":
                    item_type = ftype.get("items", "")
                    item_type = item_type.split(".")[-1]
                    ftype["items"] = item_type
                    if not self.is_known_complex_avro_type(item_type):
                        types[f["name"]] = self._map_primitive(
                            f["name"], item_type, pluralize=True
                        )
                    else:
                        types[f["name"]] = f"<{f['name']}>: [uid] ."
                        # the type alias we write into the avro schema we loaded - we cant use this again now in avro but thats ok
                        # alternatively we could add it to a graph section on the topic object
                        # we ignore type qualifiåtion such as optimus.printfile_pieces.piece_info.PrintfilePieceInfo for now at least
                        # its assumed local anyway to the file here

                        ftype["type_alias"] = self._shared_aliases.get(item_type)
                        self[self._name]["referenced_types"].append(item_type)

                elif ftype.get("type") == "map":
                    res.utils.logger.debug(
                        f"Dgraph default mapping of maps is to string"
                    )
                    types[f["name"]] = self._map_primitive(f["name"], "string")
                else:
                    # we should still handle a single instance of a complex type for example
                    raise Exception(
                        f"Avro complex type {f['type']} not yet supported - if this is a complex type make sure its registered (how to)"
                    )

        return types

    # def __repr__(self):
    #     return self.type_dql


class GraphModel(dict):
    """
    Schema management and evolution is subtle - need to think about how dgraoh does this
    - make sure we are sure about edge types
    - we add predicates and interfaces up front: both handy for configuring how things work automatically
    """

    INIT = """
<xid>: string @index(exact) @upsert .
<cascade.xid>: string @index(exact) @upsert .
<sys.modified>: datetime @index(year) .
<airtable.purge_at>: datetime @index(year) .
<id>: string @index(exact) .
<key>: string @index(exact) .
<name>: string @index(exact) .
<piece_id>: string @index(exact) .
<stitching_job_key>: string @index(exact) .
<one_number>: string  .
<brand_code>: string  .
<body_code>: string  .
<material_code>: string  .
<color_code>: string  .
<one_number>: string  .
<body_version>: string  .
<status>: string  .
<flow>: string  .
<table_id>: string  .
<base_id>: string  .
<notes>: string  .
<tags>: [string]  .
<uri>: string  .
<code>: string  .
<category>: string  .
type <lookup_type> {
xid
}"""

    """
    built ins
    - core predicates are predefined globally and cannot be changed
    - Assumed entities are also built-in keys pointing to entity types -> in graph ql these become uids or [uids] if pluralized keys
    """
    CORE_PREDICATES = [
        "xid",
        "id",
        "uid",
        "key",
        "name",
        "tags",
        "satus",
        "notes",
        "uri",
        "flow",
        "table_id",
        "field_id",
        "one_number",
        "stitching_job_key",
        "piece_id",
    ]
    ASSUMED_ENTITIES = {
        "body_code": "bodies",
        "material_code": "materials",
        "color_code": "colors",
        "brand_code": "brands",
        "one_number": "make_orders",
        "meta_one_key": "meta_ones",
        "piece_key": "meta_one_pieces",
        "one_piece_key": "make_order_pieces",
        # think about this a bit more - next two - do we need files or just events on files
        "print_file_key": "print_files",
        "print_file_name": "printfile_events",
        # "roll_key": "rolls",
        "printer_key": "printers",
        # actually this is a misnomer it should be printed files
        "stitching_job_key": "stitched_files",
    }

    def __init__(self, spec_yaml, **options):
        self._spec = (
            yaml.safe_load(spec_yaml) if isinstance(spec_yaml, str) else spec_yaml
        )

        # validate
        # -only one mapping between alias and topic

        # predicate validation is an interesting one-> we must not add strings to things that are usually floats
        self._add_edges_to_types = options.get("add_edges_to_types", True)
        self._topics = self._spec["topics"]
        l = len(self._topics)
        self._topics = {t["schema_key"]: t for t in self._topics}

        assert (
            len(self._topics) == l
        ), "Not all the keys are unique in the topics defined"
        self._shared_aliases = {}
        self._loaded_topics = {}
        res.utils.logger.debug(f"Loading graph model with topics {self.topic_names}")
        for t in self.topic_names:
            topic = self._load_topic(t)
            # we load the full object back into topics - look at the spec for the raw
            self._loaded_topics[t] = topic
            # we add each topic as its own key top level on the graph dict too
            self.update(topic)
        # load the global context and then compile

        res.utils.logger.debug(f"Aliases: {self._shared_aliases}")
        for _, v in self._loaded_topics.items():
            try:
                v.init()
            except Exception as ex:
                print(v)
                raise ex
        self._init_references()

        # add inverse edge for referenced child types in topics

    @staticmethod
    def from_schema_path(path=None):
        path = path or str(
            Path(res.utils.get_res_root()) / "res-schemas" / "res" / "topic_graph.yaml"
        )
        res.utils.logger.info(f"Loading config from {path}")
        graph_config = res.utils.read(path)
        return GraphModel(graph_config)

    @staticmethod
    def _init_graph():
        """
        populate dgraph with some data with LU types
        """
        from res.connectors.dgraph.utils import key_resolver

        airtable = res.connectors.load("airtable")
        res.utils.logger.debug("loading styles")
        df = airtable["appjmzNPXOuynj6xP"]["tblmszDBvO1MvJrlJ"].to_dataframe(
            fields=["Style Name", "Resonance_code"]
        )
        df["bodies"] = df["Resonance_code"].map(lambda s: s.split(" ")[0])
        df["materials"] = df["Resonance_code"].map(lambda s: s.split(" ")[1])
        df["colors"] = df["Resonance_code"].map(lambda s: s.split(" ")[2])

        stuff = []
        from tqdm import tqdm

        for column in ["bodies", "materials", "colors"]:
            res.utils.logger.info(f"adding {column}...")
            for field in tqdm(df[column].unique()):
                uid = key_resolver(column, field)

                stuff.append({"entity": column, "xid": field, "uid": uid})

        res.utils.logger.debug("loading sizes")
        sdf = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"].to_dataframe()
        sdf = sdf[
            ["Size Chart", "_accountingsku", "Size Category", "Size Normalized"]
        ].rename(
            columns={
                "Size Chart": "size_name",
                "_accountingsku": "key",
                "Size Category": "category",
                "Size Normalized": "res_size_code",
            }
        )

        res.utils.logger.info("adding sizes...")

        for k in sdf["key"].unique():
            uid = key_resolver("sizes", k)

        res.utils.logger.info("adding current make_orders")

    def get_type_config(self, name):
        return self._loaded_topics[name]

    def _init_references(self):
        for key, parent in self._loaded_topics.items():
            parent = parent[key]
            rtypes = parent.get("referenced_types", [])
            for key, child in self._loaded_topics.items():
                child = child[key]
                if key in rtypes:
                    r = child.get("referenced_by_type", [])
                    # add that the parent is referencing me
                    r.append(parent["schema_key"])
                    child["referenced_by_type"] = list(set(r))

    def _load_topic(self, k):
        t = self._topics.get(k)
        if t:
            a = GraphModel.load_schema_for_topic_info(t)
            res.utils.logger.debug(f"Generating the topic config for the graph for {t}")
            try:
                return TopicModel(t, a, graph=self)
            except Exception as ex:
                res.utils.logger.warn(f"Failing for {t} : {a}")
                raise ex

    @property
    def topic_models(self):
        return self._loaded_topics

    @property
    def topic_names(self):
        return list(self._topics.keys())

    @property
    def schema_table(self):
        import pandas as pd

        df = pd.DataFrame(self).T

        df["referenced_types_count"] = df["referenced_types"].map(
            lambda l: len(l) if isinstance(l, list) else 0
        )
        df["referenced_by_type_count"] = df["referenced_by_type"].map(
            lambda l: len(l) if isinstance(l, list) else 0
        )

        return df.sort_values(["name", "referenced_by_type_count"])

    @staticmethod
    def _schema_path_from_key(k):
        """
        From RES home we can try to find the
        """
        k = k.replace(".", "/").split("/")
        a = k[0].replace("_", "-")
        b = k[1].replace("_", "-")
        c = k[2]

        root = Path(RES_SCHEMA_HOME) / "avro"

        k = f"{a}/{b}/{c}.avsc"
        if (root / k).exists():
            return k
        k = f"{a}/{b.replace('-', '_')}/{c}.avsc"

        if (root / k).exists():
            return k

        raise Exception(f"Unable to determine path for {root/k}")

    @staticmethod
    def load_schema_for_topic_info(info):
        path = info.get(
            "schema_path", GraphModel._schema_path_from_key(info.get("name"))
        )
        key = info.get("schema_key")
        if not path:
            res.utils.logger.debug(f"trying to infer path")
            # TODO
        path = f"{AVRO_SCHEMA_HOME}/{path}"
        with open(path, "r") as f:
            d = json.load(f)
        if isinstance(d, list):
            if len(d) == 1:
                return d[0]
        else:
            return d
        assert (
            key is not None
        ), f"If the avro topic schema is a list with multiple elements, a schema key must be provided in the config to select an entity - see schema {path}"
        d = [t for t in d if t["name"] == key]
        assert (
            len(d) != 0
        ), f"The config is invalid - there is no type named {key} in the avro schema {path}"
        d = d[0]
        return d

    @staticmethod
    def _iterate_relationships(g, filter_entities=None):
        """
        Pass a dict or the GraphModel itself which is a dict
        iterates and finds all relationships in the config
        these are used to create edges

        returns the full raw item
        [
            {'name': 'asset_id',
            'entity_type': 'make_piece_assets',
            'cardinality': 1,
            'source_entity_name': 'roll_piece_assets'}
        ]
        """
        filter_entities = filter_entities or []
        if not isinstance(filter_entities, list):
            filter_entities = [filter_entities]
        for k, v in g.items():
            for r in v.get("relationships", []):
                d = dict(r)
                d["source_entity_name"] = k
                if not filter_entities or k in filter_entities:
                    yield d

    @staticmethod
    def iterate_all_relationships(g):
        """
        a specific single object in the graph- iterate its relationships as a map fk:fk_type where fk is the local name and fk_type is the target entity

        """
        for _, v in g.items():
            for r in v.get("relationships", []):
                yield r["name"], r["entity_type"]

    @staticmethod
    def iterate_relationships(entity_options, obj=None):
        """
        a specific single object in the graph- iterate its relationships as a map fk:fk_type where fk is the local name and fk_type is the target entity
        if an object is passed we can check existence
        """

        for d in entity_options.get("relationships", []):
            if not obj or d["name"] in obj:
                yield d["name"], d["entity_type"]

    @staticmethod
    def alias_of_type(name, property, type_options):
        options = (type_options or {}).get(name)
        if options:
            for f in options.get("fields"):
                ftype = f.get("type")
                if f["name"] == property and isinstance(ftype, dict):
                    return ftype.get("type_alias", property)
        return property

    @staticmethod
    def is_json_type(name, property, type_options):
        options = (type_options or {}).get(name)
        if options:
            for f in options.get("fields"):
                if f["name"] == property:
                    ftype = f["type"]
                    # this is the avro condition for a map right now - probably should not live here but need to test cases
                    # if its a complex type by checking the type in our shared aliases we can return false here but test later with examples
                    if isinstance(ftype, dict) and ftype.get("type") == "map":
                        return True
                    break
        return False

    @staticmethod
    def make_gql_field_from_json_field(
        f, is_key=False, is_edge=False, cardinality=1, **options
    ):
        """
        GQL types Int, Int64, Float, String, Boolean, ID and DateTime -> ! non null

        primitives are easy, lists are easy
        how do we handle complex types: better that the type is record defined - maps will be weird

        type User {
            userID: ID!
            name: String!
            lastSignIn: DateTime
            recentScores: [Float]
            reputation: Int
            active: Boolean
        }

        {{'type': 'array', 'items': 'string'}}

        """

        name = f["name"]
        type_info = f["type"]

        if isinstance(type_info, list) or isinstance(type_info, str):
            if not isinstance(type_info, list):
                type_info = [type_info]

            nullable = "null" in type_info
            nullable = "" if nullable else "!"

            major_type = [t for t in type_info if t != "null"][0]
            if is_key:
                return f"{name}: String! @id"

            if major_type in ["string", "int", "float", "boolean"]:
                return f"{name}: {major_type.capitalize()}{nullable}"
            return f"{name}: String"
        print("no mapping for ", name)
        return None

    @staticmethod
    def make_topic_gql(info, **graph_options):
        json_info = GraphModel.load_schema_for_topic_info(info)

        key_field = info["key"]
        name = info["alias"]

        gql = f"type {name}{{\n"
        for f in json_info["fields"]:
            is_edge = False  # determine from info
            cardinality = 1  # determine from edge info
            f = GraphModel.make_gql_field_from_json_field(
                f,
                is_key=f["name"] == key_field,
                is_edge=is_edge,
                cardinality=cardinality,
            )
            if f:
                gql += f" {f}\n"

        gql += "}"

        return gql

    @staticmethod
    def open(self, path):
        pass

    def reserved_predicate_keys(self):
        return GraphModel.CORE_PREDICATES + list(GraphModel.ASSUMED_ENTITIES.keys())

    def _get_uid_predicates_string(self):

        if not self._add_edges_to_types:
            return ""

        res.utils.logger.debug(f"Adding edge predicates for assumed entities")
        keys = list(GraphModel.ASSUMED_ENTITIES.keys())
        return "\n".join([f"<edge_{k}>: uid ." for k in keys])

    def get_parent_type_aliases(self):
        for _, v in self.items():
            if v.get("referenced_types", []):
                yield v["alias"]

    def _get_all_predicates_string(self):
        reserved = self.reserved_predicate_keys()
        predicates = []
        for k, t in self._loaded_topics.items():
            for k, v in t.predicates_dql.items():
                # as well as checking if its reserved we should really check for a type conflict which might require a qualifier in schema or some other action
                if k not in reserved:
                    predicates.append(v)

        # the parent nodes are ones where we have a child pointing to a parent but can the reverse to many children
        for t in self.get_parent_type_aliases():
            predicates.append(f"<edge_{t}>: uid @reverse .")

        return (
            GraphModel.INIT
            + "\n"
            + self._get_uid_predicates_string()
            + "\n"
            + "\n".join(predicates)
        )

    def make_type_dql(self, alias, fields):
        NL = "\n"
        edges_for_fields = self.edges_for_fields(fields)
        fields = edges_for_fields + fields
        return f"""type {alias}{{{NL}{NL.join(fields)}{NL}}}"""

    def _get_all_types_string(self):
        """
        merged any fields mapped to a type into the type's schema
        """
        mapper = {}
        for k, t in self._loaded_topics.items():
            alias = t[k]["alias"]
            if alias not in mapper:
                mapper[alias] = set()
            mapper[alias] |= set(t.field_names)

        return "\n".join([self.make_type_dql(k, list(v)) for k, v in mapper.items()])

    def as_dql_schema(self):
        return self._get_all_predicates_string() + "\n" + self._get_all_types_string()

    def edges_for_fields(self, fields):
        edge_keys = list(GraphModel.ASSUMED_ENTITIES.keys())
        return [f"edge_{f}" for f in fields if f in edge_keys]

    def apply_schema(self):
        """
        apply schema to database - we will get some errors that we should lear to validate ourselves e.g. missing or conflicting predicates
        this function should never complaing if we do our job
        """
        s = self.as_dql_schema()
        from res.connectors.dgraph.utils import apply_schema

        res.utils.logger.info("applying schema to dgraph server")
        return apply_schema(s)

    def query_type_by_alias(self, alias, **kwargs):
        from res.connectors.dgraph.utils import query_type

        # get alias config for type

        return query_type(name=alias, **kwargs)

    def sync_airtable_schema_from_graph_config(self, name, plan=False, **kwargs):
        """
        Using the /res_schemas/res/topic_graph.yaml format loaded as graph.get_type_config(name) sync the table
        we simply add fields that do not exist
        if we wanted to update schema fields we can do that too by renaming existing ones and creating new ones and then migrating the data and dropping the field
        - this is treated as a separate migration routine for future - for now we can manage the schema in a semi-auto way



        https://airtable.com/api/enterprise#fieldTypes
        """
        config = self._loaded_topics[name]
        fields = []
        table_name = titlecase(config._alias)
        airtable = res.connectors.load("airtable")

        key_field = None
        for f in config.primitive_fields:
            key = titlecase(f["name"])
            # by default with title case but we can choose something else
            # this is also useful if a another schema map has determined the key

            # we need to remember this in the connector to
            if f.get("is_key"):
                key_field = key
                if config._airtable_key:
                    res.utils.logger.debug(
                        f"An airtable primary key mapping {f['name']}->{config._airtable_key} was provided"
                    )
                    key = config._airtable_key
                # dont need to add it again whatever that means
                continue

            ftype = python_type_from_avro_type(f)

            d = {"name": key, "type": "singleLineText"}

            if ftype is list:
                d["type"] = "multipleSelects"
                d["options"] = {"choices": [{"name": "*"}]}

            if ftype is int:
                d["type"] = "number"
                d["options"] = {"precision": 0}
            if ftype is float:
                d["type"] = "number"
                d["options"] = {"precision": 2}
            if ftype is bool:
                d["type"] = "checkbox"
                d["options"] = {"icon": "check", "color": "greenBright"}

            if ftype is datetime:
                d["type"] = "dateTime"
                d["options"] = {
                    "dateFormat": {"name": "iso"},
                    "timeFormat": {"name": "24hour"},
                    "timeZone": "utc",
                }

            fields.append(d)

        for aa in config.airtable_attachments:
            d = {"name": titlecase(aa["name"]), "type": "multipleAttachments"}
            fields.append(d)

        fields.append({"name": "sys.primitive_hash", "type": "singleLineText"})

        # fetch metadata for the table
        b = airtable[AIRTABLE_ONE_PLATFORM_BASE]
        # create the table if it does not exist
        r = b.create_new_table(table_name, key_name=key_field, plan=plan)

        if r.get("id"):
            # quick reload for now
            b = airtable[AIRTABLE_ONE_PLATFORM_BASE]
        t = b[table_name]

        existing_fields = list(t.fields["name"])
        for f in fields:
            # we can do some sort of migration separately if the schema evolves by rename and migrate fields
            if f["name"] not in existing_fields:
                if not plan:
                    res.utils.logger.debug(
                        f"Updating field [{f['name']}] on table {table_name}"
                    )

                    try:
                        t.update_field_schema(f)
                    except Exception as ex:
                        # we will see these errors - i think this only happens if we incorrectly request something that exists like a sys field
                        pass

        return t, fields
