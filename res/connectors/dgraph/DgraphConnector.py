"""
Connector for Dgraph

-forums: https://discuss.dgraph.io/


Some example mutations to learn how to use dgraph etc
- https://github.com/dgraph-io/dgraph/blob/master/graphql/resolve/add_mutation_test.yaml

Advantage od dgraph;
-We exploit the idea that all types are triples (type,uid,key) where key is our resonance user friendly key
In the schema when we define meta_types, we can build mutations that resolve links between types automatically
the current version is just a prototype so requires testing, features and optimization but this is a promsing convention. 
-We use schema but in a soft way which is a really nice balance between eventual correctness but flexible iteration. Schema can be add 
incrementally, clients can validate against schema before writing to dgraph, we can later run processes to validate data against schema
- Adding edges between nodes means the relationships as well as the types can evolve 

Other notes:
We can use graphql or dql features. DQL is used more "under the hood" while we should expose more graphQl features to users

TODO:
-handle points to new nodes as well as existing lookups; couple of ways to do this - for now assume target e.g a material exists for key
-remove the redundant delete block or simple upserts / investigate dgraph upserts at least to one level 

"""


import json
import ast
import os
import pandas as pd
from res.utils.schema.registry import common_fields
from res.utils.dataframes import is_datetime
from .. import DatabaseConnector, DatabaseConnectorSchema, DatabaseConnectorTable
from res.utils import dataframes, safe_http, logger, dates

# http://dgraph-dgraph-alpha.dgraph.svc.cluster.local:8080
DGRAPH = os.environ.get(
    "DGRAPH_ALPHA",
    "http://localhost:8080",  # "http://dgraph-dgraph-alpha.dgraph.svc.cluster.local:8080"
)
META_ENTITY_DGRAPH_NODE_NAME = "meta.entities"

# for dgraph as a schema registry these are the res fields that are store in dgraph
# these are defined by the schema which is given as a test sample
META_ENTITY_PROPS = [
    "key",
    "name",
    "type",
    "interfaces",
    "airtable_table_id",
    "airtable_base_id",
    # "fields",
]

META_FIELD_PROPS = [
    "key",
    "name",
    "is_key",
    "is_required",
    "is_child_type",
    "type",
    "meta_type",
    "indexes",
    "airtable_field_name",
]


class DgraphConnector(DatabaseConnector):
    """
    provides some opinionated functions for interacting with Dgraph
    See notes in [platform docs] (https://coda.io/d/Platform-Documentation_dbtYplzht1S/Dgraph_suuPr#_luLq9)
    And further examples in how Dgraph is used as a type registry
    [Res meta registry](https://coda.io/d/Platform-Documentation_dbtYplzht1S/Res-meta-schema-registry_suB5M#_lu-qI)

    """

    PYTHON_TYPE_MAP_GQL = {
        "int": "Int",
        "str": "String",
        "datetime": "DateTime",
        "bool": "Boolean",
        "float": "Float",
        "list[str]": "[String]",
        "list[int]": "[Int]",
        "object": "String",
    }

    # https://dgraph.io/docs/query-language/schema/
    PYTHON_TYPE_MAP = {
        "int": "int",
        "str": "string",
        "datetime": "dateTime",
        "bool": "bool",
        "float": "float",
        "list[str]": "[string]",
        "list[int]": "[int]",
        "object": "string",
        "polygon": "geo",
        "point": "geo",
        "multipoint": "geo",
        # "image" : ""
    }

    def __init__(self):
        pass

    def __getitem__(self, name):
        return DgraphConnectorTable(name)

    @staticmethod
    def install(**kwargs):
        # TODO:
        # install predicates for all common types
        # register meta entities entry in meta entities so dg['meta.entities] can create mutations
        # etc.
        pass

    @staticmethod
    def query_item(query, filters=None, json_properties=None):
        """
        Example query:>
        {
            get(func: type(res_types))
            @filter(eq(type_name, "styles"))
            {
              type_name
              airtable_table_id
              airtable_base_id
              fields{
                name
                type
                airtable_field_name
              },
              child_types
            }
          }
        """

        response = safe_http.request_post(
            f"{DGRAPH}/query",
            data=query,
            headers={"Content-Type": "application/dql"},
        )

        # do some checks that we successfully got an item

        result = response.json()["data"]["get"][0]

        return result

    @staticmethod
    def query_items(query, filters=None, json_properties=None):
        """ """

        response = safe_http.request_post(
            f"{DGRAPH}/query",
            data=query,
            headers={"Content-Type": "application/dql"},
        )

        # do some checks that we successfully got an item

        result = response.json()["data"]["get"]

        return result

    @staticmethod
    def apply_type_conventions(
        k, v, name, child_types=None, list_types=None, force_complex_to_str=True
    ):
        """
        apply conventions for nested types
        pass in a field, value on an entity

        if we have not defined some json nesting is a sub type it is treated as a string
        and for types and child types we set their dgraph.type on insert
        """
        child_types = child_types or {}

        if k in child_types:
            # TODO regex cleaner here but also check why its needed
            ctype = child_types[k].replace("[", "").replace("]", "")
            # if its a string but it is supposed to be a type, assume json string
            if isinstance(v, str):
                v = json.loads(v)
            if not isinstance(v, list):
                v["dgraph.type"] = [f"{name}.{ctype}"]
                # one level recusion
                v = {
                    _k: DgraphConnector.apply_type_conventions(_k, v, ctype)
                    for _k, v in v.items()
                }
            else:
                for item in v:
                    item["dgraph.type"] = [f"{name}.{ctype}"]
                    # one level recursion
                    item = {
                        _k: DgraphConnector.apply_type_conventions(_k, v, ctype)
                        for _k, v in item.items()
                    }

        elif isinstance(v, dict) or isinstance(v, list):
            # first check not in a list type that we allow
            if k not in (list_types or []):
                if force_complex_to_str and k not in ["dgraph.type"]:
                    # if dgraph type is inside remove it as its no longer a type
                    if "dgraph.type" in v:
                        v.pop("dgraph.type")
                    v = str(v)

        return dataframes.coerce_null(v)

    @staticmethod
    def get_entity_by_key_lookup(etype, key):
        def f(
            key_field="key",
        ):
            query = f"""{{
                    get(func: type({etype}))
                    @filter(eq(key, {key}) )
                    {{
                    uid
                    expand(_all_)
                    }}
                }}"""

            return DgraphConnector.query_items(query)

        return pd.DataFrame(f(etype))

    @staticmethod
    def lookup_keys_by_key_value(etype, key, value):
        def get_keys(key_field="key"):
            query = f"""{{
                    get(func: type({etype}))
                    @filter(eq({key}, {value}) )
                    {{
                    uid
                    {key_field}
                    }}
                }}"""

            return DgraphConnector.query_items(query)

        return pd.DataFrame(get_keys())

    @staticmethod
    def _delete_entity_by_key_value(etype, key, value, plan=False):
        def make_query(key_field="key"):
            query = f"""{{
                    get(func: type({etype}))
                    @filter(eq({key}, {value}) )
                    {{
                    uid
                    {key_field}
                    }}
                }}"""

            query = {"query": query, "mutations": [{"delete": [{"uid": "uid(v)"}]}]}

            logger.debug(
                f"deleting entities by key-value using query {query} \n [plan={plan}]"
            )

            if plan == True:
                return query
            return DgraphConnector.json_mutate(query)

        return make_query()

    @staticmethod
    def json_mutate(query):
        r = safe_http.request_post(
            f"{DGRAPH}/mutate?commitNow=true&upsert=true",
            data=json.dumps(query),
            headers={"Content-Type": "application/json"},
        )
        # report errors
        return r.json()

    @staticmethod
    def create_update_mutation(
        record,
        name,
        key="key",
        foreign_entities=None,
        child_types=None,
        interfaces=None,
        list_types=None,
    ):
        """
        if yu want to see what a mutation looks like the recommended wa is to call update_record with plan = True on the dgraph resolved type connector

        record: a Json payload possibly nested
        name: the meta or res name of the entity
        key: the key field assumed 'key' by convention
        forign_entities: a collection of properties describing an fk relation
                        [{"meta_name":"the meta name of the entity", "name": "the local field linking foreign key" }]
                        the "key" is again optional on the foreign entity descriptor as convention is to use 'key'
        child_types: a map of fields and their meta types for example {"owner" : "Person"}
        interfaces: DGRAPH types can be multiple which allows us to implement "interfaces". A type can be an `animal` and a `dog`
                    for child types do we not support interfaces as these are always associated with the parent and do not get more complex
        """

        def clean_type_name(n):
            # todo
            return n.replace("[", "").replace("]", "")

        def _has_value(x):
            if isinstance(x, list):
                if len(x) == 0:
                    return False
            # just in case
            if x == "[]":
                return False
            if pd.isnull(x):
                return False
            return True

        if interfaces is not None and isinstance(interfaces, str):
            # it should be but while in flux want to be sure as we sometimes save complex types as strings
            interfaces = ast.literal_eval(interfaces)

        record["dgraph.type"] = [name] + (interfaces or [])
        # add res metadata
        record["res.timestamp"] = dates.utc_now_iso_string()

        record = {
            k: DgraphConnector.apply_type_conventions(
                k, v, name, child_types, list_types
            )
            for k, v in record.items()
        }

        # we assume by convention the key is a string - but lets say its not, assume its an id integer
        key_val = f'"{record[key]}"' if isinstance(record[key], str) else record[key]

        # create the upsert query for this entity - ths will lookup if it exists either a single key on eq or a lookup on a list
        queries = [f"q(func: type({name})) @filter(eq({key}, {key_val})) {{ v as uid}}"]

        for lu in foreign_entities or []:
            # lookup another type using the res.meta schema for field descriptors
            # for example styles.body_code -> bodies.key would be
            # meta_type = bodies, key='key', name='body_code'
            # note the default key is always 'key' on foreign keys
            fk_table, fk, fk_field, is_required = (
                clean_type_name(lu["meta_type"]),
                lu.get("fk", "key"),
                lu["name"],
                lu.get("is_required", False),
            )

            if record.get(fk_field) == None:
                op = lambda x: "do nothing" if not is_required else logger.error
                op(
                    f"The FK field is missing or null. Typically these fields are important and should be set but they can be null. Be sure this is expected for {name} record with key {key_val}"
                )
                continue

            fk_value = (
                f'"{record[fk_field]}"'
                if isinstance(record[fk_field], str)
                else record[fk_field]
            )

            # for lists do this if the key val is a list and we need a lookup
            if isinstance(fk_value, list) and len(fk_value) > 0:
                # fk_value = "[" + ",".join([f'"{k}"' for k in fk_value]) + "]"
                fk_value = ",".join([f'"{t}"' for t in fk_value])

            # only use a lookup if we have something to find

            filter = (
                f"@filter(eq({fk},{fk_value}) ){{lookup_{fk_field} as uid}}"
                if _has_value(fk_value)
                else ""
            )

            queries.append(f"get_{fk_table}(func: type({fk_table})) {filter}")

        query = " ".join(queries)

        # can we should we?
        record["uid"] = "uid(v)"

        query = {
            "query": f"{{ {query} }}",
            "mutations": [
                {
                    # subtle: we must remove child types and recreate them when updating a record
                    # you need the uid think in edge deletion but the second uid added at end probably not
                    "delete": [{"uid": "uid(v)", k: None} for k in child_types or []]
                    # do we need this anymore? -> check the uid generated on the node with and without this - this seems like a brutal sort of "replace" but is the id removed or not is the question
                    # + [{"uid": "uid(v)"}],
                    ,
                    "set": {k: v for k, v in record.items()},
                }
            ],
        }

        for lu in foreign_entities or []:
            # the kf field is what we use to look up but we "create" a field for the type
            fk_field = lu.get("name")
            # need to be careful with what the name is called
            # this appears in the left hand side of meta type links
            edges = clean_type_name(lu["meta_type"])
            # the warning for this omission is above but we only add if we have a value
            if record.get(fk_field) and not lu.get("is_child_type"):
                query["mutations"][0]["set"][edges] = {"uid": f"uid(lookup_{fk_field})"}

        # logger.debug(query)

        return query

    @staticmethod
    def create_update_mutation_for_type(record, name, **kwargs):
        schema = DgraphConnector.try_load_meta_schema(name)
        key_field = kwargs.get("key", "key")
        name = schema.get("key")

        # any field marked as a child type must have a meta type
        child_types = {
            f["name"]: f["meta_type"]
            for f in schema.get("fields", [])
            if f.get("is_child_type", False)
        }
        # anything that is not a child and has a meta type is a lookup FK
        # we store the entire payload
        foreign_entities = [
            f
            for f in schema.get("fields", [])
            if f.get("meta_type") and f.get("name") not in child_types
        ]

        # list types are types that we allow as lists in dgraph
        list_types = [
            f["name"] for f in schema.get("fields", []) if "[" in f.get("type", "")
        ]

        # if len(child_types):
        #     logger.debug(f"Found child types in the registry {child_types}")

        # if len(foreign_entities):
        #     logger.debug(f"Found foreign key types in the registry {foreign_entities}")

        mutation = DgraphConnector.create_update_mutation(
            record,
            name=name,
            key=key_field,
            foreign_entities=foreign_entities,
            child_types=child_types,
            list_types=list_types,
            interfaces=schema.get("interfaces", []),
        )

        return mutation

    @staticmethod
    def update_record(record, name, schema=None, plan=False, **kwargs):
        def clean_type_name(n):
            # todo
            return n.replace("[", "").replace("]", "")

        schema = schema or {}
        key_field = kwargs.get("key", "key")

        # any field marked as a child type must have a meta type
        child_types = {
            f["name"]: clean_type_name(f["meta_type"])
            for f in schema.get("fields", [])
            if f.get("is_child_type", False)
        }
        # anything that is not a child and has a meta type is a lookup FK
        # we store the entire payload
        foreign_entities = [
            f
            for f in schema.get("fields", [])
            if f.get("meta_type") and f.get("name") not in child_types
        ]

        # list types are types that we allow as lists in dgraph
        list_types = [
            f["name"] for f in schema.get("fields", []) if "[" in f.get("type", "")
        ]

        # if len(child_types):
        #     logger.debug(f"Found child types in the registry {child_types}")

        # if len(foreign_entities):
        #     logger.debug(f"Found foreign key types in the registry {foreign_entities}")

        mutation = DgraphConnector.create_update_mutation(
            record,
            name=name,
            key=key_field,
            foreign_entities=foreign_entities,
            child_types=child_types,
            list_types=list_types,
            interfaces=schema.get("interfaces", []),
        )

        # print(mutation)

        if plan == True:
            return mutation

        response = safe_http.request_post(
            f"{DGRAPH}/mutate?commitNow=true", json=mutation
        )
        errors = response.json().get("errors", None)
        if errors:
            logger.warn(f"Could not apply this mutation: {mutation}")
            raise Exception(f"Failed to write record to dgraph because + {errors}")

        return response

    @staticmethod
    def alter_schema(schema):
        """
        There are a number of ways to update schema in dql/graphql and im not sure about the differences
        The alter schema endpoint seems pretty intuitive where we can define predicates and types

        Global predicates like key, name, status, created, updated have "shared" predicates
        We need to be careful with name collisions so we full qualify certain non common fields etc.

        Example:

            name: string @index(exact) .
            regnbr: string @index(exact) .
            <Cars.owner>: uid @reverse .

            type Persons {
            name
            }
            type Cars {
            regnbr
            Cars.owner
            }


        https://dgraph.io/docs/query-language/schema/
        """
        options = "?runInBackground=true"
        r = safe_http.request_post(f"{DGRAPH}/alter{options}", data=schema)
        errors = r.json().get("errors", None)
        if errors:
            logger.warn(f"Could not apply this mutation: {r}")
            raise Exception(f"Failed to write record to dgraph because + {errors}")

        logger.info(f"schema updated")
        return r.json()

    @staticmethod
    def update_schema(schema):
        r = safe_http.request_post(
            f"{DGRAPH}/admin/schema",
            data=schema,
            headers={"Content-Type": "application/dql"},
        )
        errors = r.json().get("errors", None)
        if errors:
            logger.warn(f"Could not apply this mutation: {r}")
            raise Exception(f"Failed to write record to dgraph because + {errors}")

        logger.info(f"schema updated")
        return r.json()

    @staticmethod
    def get_augmented_python_field_metadata(fields, **kwargs):
        """
        TODO: This should be abstracted into some sort of schema manager shared by "providers" and also we would store these data directly in a database

        Returns in the python format that can later be mapped. This represents all the matadata we *could* store about a field accross systems
        But as we iterate we will add provider-specific augmentations in each provider and merge in a database later
        """

        def make_field(k, v):
            d = {
                "name": k,
                "type": v,
            }

            if (
                "is_key" in kwargs and k in kwargs["is_key"]
            ):  # returns a list of matches
                d["is_key"] = True

            if (
                "is_required" in kwargs and k in kwargs["is_required"]
            ):  # returns a list of matches
                d["is_required"] = True

            return d

        return [make_field(k, v) for k, v in fields.items()]

    @staticmethod
    def make_predicates_for_field(f, entity_name):
        """
        Can create multiple predicates because we add a FK link

        uid @reverse . <- if we do this e.g. on styles
        we can do
          bodies {
              ~styles {key}
          }
        """
        preds = {}
        name = f["name"]
        # indexes merged into the same list

        # string is default type
        type_name = DgraphConnector.PYTHON_TYPE_MAP.get(f.get("type", "str"))
        # for meta types e.g. res entities we are going to create and edge
        if "meta_type" in f:
            meta_type = f.get("meta_type")
            # check if its a list based on the def of the simple type
            is_list = True if "[" in type_name or "[" in meta_type else False
            # the type without the list is what we want for the meta type
            meta_type = meta_type.replace("[", "").replace("]", "")
            # This is important or we always loose multiple edges and get one uid entry UNIT TEST
            otype = "uid" if not is_list else "[uid]"
            oname = f"{entity_name}.{meta_type}"

            # test - self ref links (we probably dont need the predicate at all)
            if entity_name == meta_type:
                oname = entity_name

            preds[oname] = f"<{oname}>: {otype}  ."

        indexes = (
            ""
            if "indexes" not in f or f.get("meta_type")
            else f" @index({','.join(set(f['indexes']))})"
        )

        # we add the type if its not a child type - this is because for lookups we want both the code and the link but for child types we dont
        if not f.get("is_child_type"):
            preds[name] = f"<{name}>: {type_name}{indexes} ."

        return preds

    @staticmethod
    def make_dql_schema_from_meta_type(
        res_meta_schema, ignore=None, skip_common_field_predicates=True
    ):
        """
        give the dictionary payload for res meta, define a type and all its sub types in dql schema notation
        the type and subtypes are stored in the dictionary. the root type is denoted by `name`
        this can be applied with `apply_schema` to create the type

        Apply conventions:
        - if its a standard type dont add a predicate - but assume exists (how do we check?)
        - if its a type create in <>

        #predicate format example
        f"name: string @index(fulltext, term) ."
        """

        # assume a set in general
        if not isinstance(res_meta_schema, list):
            res_meta_schema = [res_meta_schema]

        def key_to_name(k):
            return k.split(".")[-1] if k else None

        predicates, types = {}, []
        for info in res_meta_schema:
            name = info.get("name")
            key = info.get("key")
            # get the field names from either the name or they key if supplied
            field_names = [
                f.get("name", key_to_name(f.get("key"))) for f in info["fields"]
            ]
            for i, f in enumerate(info["fields"]):
                field_name = field_names[i]
                # common fields are stored in the registry as things that we create predicates for on 'install'
                if (
                    field_name not in common_fields
                    or skip_common_field_predicates == False
                ):
                    predicates.update(
                        DgraphConnector.make_predicates_for_field(f, name)
                    )

            # we can generate fields from the predicate checks e.g. when we find FKs or child types
            # some fields e.g. common ones dont have predicates so we add those too
            field_names = list(set(field_names) | set(predicates.keys()))
            field_names = "\n  ".join(field_names)
            types.append(f"""type {key}{{\n  {field_names}\n}} \n""")

        types = "\n".join(types)
        predicates = "\n".join(predicates.values())

        return f"{predicates}\n\n{types}"

    @staticmethod
    def make_gql_schema(name, fields, ignore=None):
        """
        #DEPRECATE this one
        NOTE:
        **
        there are two schema formats and ways to update them (i think) - this is the graph ql format??
          use this with update_schema when updating
        (the other is dql schema - use with alter_schema)
        this is confusing but this confusion is merely proagated from the alterantives in dgraph
        **
        Given the name of the entity and some fields using our standard field descriptor syntax which may change
        an entry looks something like this

            {
                "name": "code",
                "airtable_field_name": "Key",
                "type": "str",
                "is_required": True,
                "indexes": ["hash"],
                "is_key": True,
            }
        """

        pmap = DgraphConnector.PYTHON_TYPE_MAP_GQL

        def make_predicate_for_field(f):

            s = f"{f['name']}: {pmap[f['type']] if f['type'] in pmap else f['type']}"
            if f.get("required", False) or f.get("is_key", False):
                s += "!"
            if f.get("is_key", False):
                s += " @id"
            if "indexes" in f:
                s += f" @search(by: [{','.join(f['indexes'])}])"
            if "inverse_link" in f:
                s += f" @hasInverse(field: {name})"
            return s

        # i dont know if we need to do this but we load the external type defs
        # so we can refer to these types in the schema for the dependant
        # TODO in the new schema type is actually used as meta type as types really are just python types

        types = set([f.get("meta_type") for f in fields])
        external_types = [f for f in types if f is not None]
        types = ""
        if external_types:
            logger.debug(
                f"The following were interpreted as external types which may not be the case {external_types}"
            )
            types = []
            for et in external_types:
                if (ignore and et in ignore) or et in ["uid", "[uid]"]:
                    continue
                # when looking up other types pass self to prevent recursions
                try:
                    # these types need to exist in the dgraph metastore OR some metastore
                    types.append(DgraphConnectorTable(et)._make_schema(ignore=name))
                except:
                    raise Exception(f"Failed to lookup an external type {et}")
            types = "/n".join(types)
        ############################

        fields = "\n  ".join([make_predicate_for_field(f) for f in fields])
        return f"""type {name}{{\n  {fields}\n}} \n{types}"""

    @staticmethod
    def try_load_meta_schema(key, meta_name="meta.entities"):
        """
        For the named entity, look into dgraph as the schema registry and get the schema
        """

        # the meta one does not need to be store
        if key == meta_name:
            return {
                "key": meta_name,
                "fields": [
                    {
                        "name": "fields",
                        "is_child_type": True,
                        "meta_type": "meta.fields",
                    }
                ],
            }

        entity_props = "\n".join(META_ENTITY_PROPS)
        field_props = "\n".join(META_FIELD_PROPS)

        q = f"""{{
              get(func: type({meta_name})) 
              @filter(eq(key,"{key}"))
              {{
                {entity_props}
                fields{{
                  {field_props}
                }}
              }}
        }}"""

        r = safe_http.request_post(
            f"{DGRAPH}/query", data=q, headers={"Content-Type": "application/dql"}
        )

        errors = r.json().get("errors", None)
        if errors:
            logger.warn(f"Could not apply this mutation: {r}")
            raise Exception(f"Failed to write record to dgraph because + {errors}")

        result = r.json()["data"]["get"]
        if len(result) == 0:
            logger.warn(
                f"There is no matching meta entry for {key} - returning an empty dict"
            )
            return {}
        return result[0]

    def query(self, q, **kwargs):
        r = safe_http.request_post(
            f"{DGRAPH}/query", data=q, headers={"Content-Type": "application/dql"}
        )

        errors = r.json().get("errors", None)
        if errors:
            logger.warn(f"Query failed: {r}")
            raise Exception(f"FQuery failed: {errors}")

        result = r.json()["data"]["get"]
        if len(result) == 0:
            logger.debug(f"No results returned")
            return {}

        return result

    def query_dataframe(self, q, **kwargs):
        """
        graphql query to dataframe result
        - Some helpers for expanding nested results

        """
        df = pd.DataFrame(self.query(q, **kwargs))

        if kwargs.get("expand_levels"):
            df = dataframes.expand_levels(df, kwargs.get("expand_levels"))

        return df

    @staticmethod
    def _apply_meta_schema_payload_conventions(payload):
        # a type must have a key field - a name is optional
        entity_key = payload.get("key")
        for f in payload["fields"]:
            # the name should always be unqualified and defaults to the value from key
            f["name"] = f.get("name", f.get("key", "").split(".")[-1])
            # the key is always the qualified name
            f["key"] = f"{entity_key}.{f['name']}"

        return payload

    @staticmethod
    def update_type_from_metatype(
        name, meta_schema=None, provider=None, meta_entity_table="meta.entities"
    ):
        """
        This is the primary workflow for creating metatypes from the provider
        We allow meta_schema to be null so we can test loading from what is stored
        OR
        we can pass in a res meta schema and save and reload it
        """
        provider = provider or DgraphConnector
        # update the schema if its newer
        if meta_schema is not None:
            # this is just to support easier data entry and add defaults e.g. omitting keys or names and other defaults
            meta_schema = DgraphConnector._apply_meta_schema_payload_conventions(
                meta_schema
            )
            logger.info(f"Saving the res meta type {name} to dgraph")
            provider()[meta_entity_table].update_record(meta_schema)
        # the meta type is loaded here to create the schema and also within the connector instances for metadata e.g. creating mutations
        else:
            meta_schema = DgraphConnector.try_load_meta_schema(name)
        logger.info(f"Altering dgraph type from the meta type")
        schema = DgraphConnector.make_dql_schema_from_meta_type(meta_schema)
        return DgraphConnector.alter_schema(schema)


class DgraphConnectorSchema(DatabaseConnectorSchema):
    def __init__(self):
        pass


class DgraphConnectorTable(DatabaseConnectorTable):
    """

    The connector table binds to a specific table. Use this to exploit meta data
    The static methods on the DgraphConnector can do most of with this can do and this simply wraps metadata for convenience
    For example the entity name is passed to all functions or the info for creating mutations is loaded from the provider

    To avoid confusion make sure to push the latest version of the res meta schema to update both the stored metadata and the dgraph type schema itself

    #Eg.
    ```
        style_spec = res.utils.read("res_tests/.sample_test_data/res_meta_schema/styles.yaml")
        DgraphConnector.update_type_from_metatype("styles", style_spec)
        #^this updates both the metadata stored in dgraph (as the provivider) and also the schema itself
        #the dql schema is generated from the res meta schema and then altered in the datbase
    ``
    `
    The Dgraph connector is a not-fully-fleshed out client for Dgraph that abstract CRUD
    Dgraph has ALPHA nodes and ZERO nodes which need to be accessible. Locally open ports to both with Kubeforwarder
    To test local the `ratel` client can also be used to interact with a dgraph instance for which ports are opened

    Columnar and flat relational data stored in dgraph can be managed by this Connector
    It basically maps between structured pandas data and dgraph and may handle more complex types in some instances
    Think of it as a simple way to dump columnar data to dgraph without thicking too much about data structures

    """

    def __init__(self, name, **kwargs):
        self._dgraph = os.environ.get(
            "DGRAPH_ALPHA", "http://dgraph-dgraph-alpha.dgraph.svc.cluster.local:8080"
        )
        self._name = name
        self._child_types = []
        self._lookups = {}
        # res convention
        self._id_field = "key"
        self._meta_schema = self.get_schema_metadata()

    def _get_field_info(self, name):
        for f in self._schema["fields"]:
            if f["name"] == name:
                return f
        return None

    def get_schema_metadata(self, **kwargs):
        """
        There are two types of schema: the res schema system is the core one referred to here
        We can sync the graph ql schema with that. TODO clean up concepts and naming
        """
        # TODO: assume the provider is dgraph for now - this can change
        return DgraphConnector.try_load_meta_schema(self._name)

    def update_schema(self):
        """
        Derive the schema from the dataframe and any metadata we store in our RES_SCHEMA_REG
        post to the endpoint using the static method for a generic schema
        """
        return DgraphConnector.update_schema(self.get_schema())

    def get_schema(self):
        return self._make_schema()

    def create_or_update_res_schema(
        self, entity, namespace, python_field_types, **kwargs
    ):
        """
        This is a provider interface method that calls into the dgraoh type creation
        First we need to take the raw python types and create the basic schema details for the entity represented in a generic format
        but there are lots of dgraph specific things that we can add which would typically be stored in the metastore
        The metastore can be any provider but we will use dgraph as the default provider here. This means dgraph stores
        Info about types, keys, predicates, relationships etc (in dgraph). So this meta usage means we load this type info from dgraph
        To create the types for each entity in dgraph

        note: namespace not currently use on our Dgraph connector as not sure how qualifiers work
        """
        res_fields = DgraphConnector.get_augmented_python_field_metadata(
            python_field_types, **kwargs
        )
        schema = DgraphConnector.make_gql_schema(entity, fields=res_fields)
        return DgraphConnector.update_schema(schema)

    # TODO with tenacity for rpc errors against the server
    def update_record(self, record, schema=None, plan=False, **kwargs):
        """
        Assume schema loaded from meta store for instance connectors
        However this can be override or supplied if it is now stored in the meta store
        """
        schema = schema or self._meta_schema
        return DgraphConnector.update_record(
            record, name=self._name, schema=schema, plan=plan, **kwargs
        )

    def update(self, df, **kwargs):
        """
        Synonym for update_dataframe - working on interface
        """
        return self.update_dataframe(df, **kwargs)

    def update_dataframe(
        self,
        df,
        on_error="raise",
        show_progress=False,
        **kwargs,
    ):
        """
        Given a dataframe, infer all lists of dicts are subtypes or used the override
        These subtypes are extract from the records

        To split a child out of the form key, parent_fields,,, child_field (and add fk to child pass kwargs)
        - child_field a list column holding the child collections
        - parent_fk_name a singular entity name for the parent e.g. body_key for bodies
        - child_meta_type e.g. bodies

        This is useful just to send two updates one for the parent and one for the child collection
        This could be one nested call and we only do this because the provider does not handle nested edge resolution yet

        TODO -auto add meta fields that are not in the schema but always queryable like created dates and different set hashes
             - add preemptive [uid] lookups particular for things like material codes in batches
        """
        res = {}
        count_errors = 0
        parent_fk_Field = kwargs.get("parent_fk_name")
        child_meta_type = kwargs.get("child_meta_type")
        if child_meta_type:
            logger.debug(
                f"splitting out child {child_meta_type} using column {kwargs.get('child_field')} and linking it to the parent via {parent_fk_Field}"
            )
            # should pass parent_fk_name and child_field in kwargs
            df, child_df = dataframes.separate_header_columns(df, **kwargs)

        gen = df.to_dict("records")

        date_fields = [c for c in df.columns if is_datetime(df[c])]

        if show_progress:
            from tqdm import tqdm

            gen = tqdm(gen)

        for record in gen:
            for k in date_fields:
                # timestamp are still sent after dict records and this is never what you want
                # TODO: i still want to look into how dates should be handled in the interface
                record[k] = record[k].to_pydatetime().replace(tzinfo=None).isoformat()
            try:

                # purge explicitly child items until we update the lower level connect
                # e.g. pieces that are children of specs need to be removed and recreated so no orphans left behind - now this is a temp thing
                # we delete the child type along the foreign key link
                # the key field is assumed to be the key

                #############################################################################
                # unit test this mode of parent child split: do we actually delete the child types
                self.update_record(record, **kwargs)
            except Exception as ex:
                if on_error == "raise":
                    raise (ex)

                logger.warn(
                    f"Failed to write {self._name} record with id {record[self._id_field]} to dgraph"
                )
                count_errors += 1

        res["errors"] = count_errors

        if child_meta_type:
            DgraphConnector._delete_entity_by_key_value(
                child_meta_type, parent_fk_Field, record["key"], plan=True
            )

            connector = DgraphConnector()[child_meta_type]
            try:
                connector.update(child_df)
            except Exception as ex:
                logger.debug(
                    f"failed to save the child with columns {list(child_df.columns)}: {repr(ex)}"
                )
                raise

        return res

    def _delete_all_nodes(self, plan=False):
        q = {
            "query": f"{{ q(func: type({self._name}))  {{v as uid}} }}",
            "mutations": [{"delete": [{"uid": "uid(v)"}]}],
        }

        if plan:
            return q

        logger.info(f"Deleting all nodes of type {self._name}")

        return DgraphConnectorTable.json_mutate(q)

    def load_dataframe(self, filters=None, json_properties=None):
        pass

    def query(self, query, filters=None, json_properties=None):
        pass

    def get(self, key):
        q = f"""
        {{
            get(func: type({self._name}))
            @filter(eq(key, "{key}"))
            {{
              expand(_all_){{

              }}
            }}
          }}
            """
        return DgraphConnector.query_item(q)

    @property
    def existing_keys(self):
        def get_keys(etype="make.one_specification", key_field="key"):
            query = f"""{{
                get(func: type({etype}))
                {{
                uid
                {key_field}
                }}
            }}"""

            return DgraphConnector.query_items(query)

        # TODO: convert name is also FQ and the Key is still the default
        return pd.DataFrame(get_keys(self._name))

    @staticmethod
    def json_mutate(query):
        # deprecate in favour of adding to connector
        r = safe_http.request_post(
            f"{DGRAPH}/mutate?commitNow=true&upsert=true",
            data=json.dumps(query),
            headers={"Content-Type": "application/json"},
        )
        # report errors
        return r.json()

    def _make_schema(self, ignore=None):
        """
        Give the schema from the type system - prepare for publush to graph schema
        uses the instances metadata and calls the static method

        """
        return DgraphConnector.make_gql_schema(
            self._name, self._schema["fields"], ignore
        )
