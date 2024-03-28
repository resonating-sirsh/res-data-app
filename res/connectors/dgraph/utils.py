import json
import os


import res
from res.connectors.airtable.AirtableConnector import RES_ENV
from res.utils.dates import utc_now_iso_string
from res.utils import safe_http
from res.utils import hash_dict
from res.connectors.redis.RedisConnector import RedisConnectorTable
from res.connectors.dgraph.GraphModel import GraphModel
from stringcase import camelcase, titlecase
import pandas as pd

# http://dgraph-dgraph-alpha.dgraph.svc.cluster.local:8080
DGRAPH = os.environ.get(
    "DGRAPH_ALPHA",
    "http://localhost:8080",  # "http://dgraph-dgraph-alpha.dgraph.svc.cluster.local:8080"
)

SYS_FIELDS = [
    "uid",
    "xid",
    "airtable.rid",
    "airtable.purge_at",
    "airtable.purge_confirmed_at",
    "sys.primitive_hash",
    "sys.modified",
]

QUERY_TIMEOUT = 30
MUTATION_TIMEOUT = 10


class _table_cache:
    def __init__(self, name):
        self._name = name
        self._cache = RedisConnectorTable("__dgraph_cache__", name)

    def __getitem__(self, key):
        uid = self._cache[key]
        if not uid:
            uid = key_resolver(self._name, key)
            res.utils.logger.debug(uid)
            self._cache[key] = uid

        return uid


class DCache:
    def __init__(self):
        pass

    def __getitem__(self, ntype):
        return _table_cache(ntype)


def is_list_of(d, t):
    if isinstance(d, list):
        if len(d):
            if isinstance(d[0], t):
                return True
    return False


def traverse_dict_apply(d, name, action, type_options=None, path=None, out_fks=None):
    """
    traverses a dictionary visiting nodes by root name
    the path is maintained as we use this information in the tree
    the options are passed in to control what fields we are looking for e.g. type info
    we treat dicts or lists of dicts as nested types that can be operated on for dgraph type info
    """
    d, path = action(
        d, name=name, path=path, type_options=type_options, out_fks=out_fks
    )

    for k, v in d.items():
        # we check for any type alias for types when they are complex
        k = GraphModel.alias_of_type(name, k, type_options)

        if isinstance(v, dict):
            # in some case we can cast dict to json and continue
            if GraphModel.is_json_type(name, k, type_options):
                d[k] = json.dumps(v)

            traverse_dict_apply(
                v,
                k,
                action,
                type_options=type_options,
                path=path,
                out_fks=out_fks,
            )
        elif is_list_of(v, dict):
            for item in v:
                # in this case the k=type is a pluralisation of types
                traverse_dict_apply(
                    item,
                    k,
                    action,
                    type_options=type_options,
                    path=path,
                    out_fks=out_fks,
                )

    return d


def is_complex_type(v):
    """
    its not clear if we should treat dicts as complex type if they are acutally mapped to json strings
    """
    return isinstance(v, dict) or is_list_of(v, dict)


def has_complex_types(d):
    for _, v in d.items():
        if is_complex_type(v):
            return True
    return False


def list_complex_types(d):
    t = []
    for k, v in d.items():
        if is_complex_type(v):
            t.append(k)
    return t


def primitives(d, excluded=["xid", "uid"]):
    return {
        k: v
        for k, v in d.items()
        if "." not in k and k not in excluded and not is_complex_type(v)
    }


def add_hash_for_fields(d, excluded=["xid", "uid"]):

    d["sys.primitive_hash"] = hash_dict(primitives(d))
    return d


def get_asset_key(asset, name, graph_config):
    options = (graph_config or {}).get(name, {})

    key = options.get("key", "key")
    key_val = asset.get(key)
    assert (
        key_val is not None
    ), f"Unable to resolve a key value for {name}.{key} - is the config correct?"

    return key_val


def action(d, name, path=None, type_options=None, out_fks={}):
    """

    test: name should alwayds be an alises typed in dgraph? this is how we map the graph types and this is how we add the types as dgraph.type

    default action for how we want to augment objects for dgraph
    augmentation is a way to control the graph for deeply nested structures in a way that suits our mutations and queries
    its an opionated data modelling approach for no-code insertion of a graph of kafka topics or other objects

    we require type settings or we will just insert data that can be orphaned -> understand how dgraph deals with nested objects and type system

    """
    # path are a list [(type,key), (type,key)] of the objects in the tree -> the keys are not really used as they are determined by functions in the db
    path = list(path) if path else []
    is_root = len(path) == 0
    # the key of the root if we need it
    root_ref = path[0][1] if not is_root else name
    # the name of the root
    root_type = path[0][0] if not is_root else name
    options = (type_options or {}).get(name, {})

    # the default key is key -> ids on queues may bot be business keys
    key = options.get("key", "key")
    valid_key = d.get(key)
    key_val = d.get(key, "res.orphaned_node")

    if valid_key:
        path.append((name, d[key]))
    else:
        res.utils.logger.warn(
            f"You are trying to upsert a nested object {name} that has no configured schema/type - maybe a dict needs to be mapped to a JSON string?"
        )

    # we can auto generate from the type of the parent or alias but doing this for now
    # parent = "parent"

    d["dgraph.type"] = [name, f"{root_type}_cascade"]
    d["xid"] = key_val
    d["cascade.xid"] = path[0][1]
    # the uid of the parent for upserts or a blank node that we dont use
    d["uid"] = "uid(root)" if is_root else f"_:{key_val}"
    d["sys.modified"] = utc_now_iso_string(None)

    # now add the link - we could chose to replace or add ; better to add because without expanding we fall back to other data
    for fk, fk_type in GraphModel.iterate_relationships(options, d):
        # qualify
        value = d.get(fk)
        if value:
            qual_fk = f"{name}_{fk}_TO_{fk_type}_{value}"
            # a value lookup specifically
            # fk is unique property on d but the fk must be qualified to uniquely for the entire tree
            d[f"edge_{fk}"] = f"uid(fk_{qual_fk})"
            out_fks[(qual_fk, fk_type)] = value
            res.utils.logger.debug(f"Adding rel {(qual_fk, fk_type)}:{value}")

    d = add_hash_for_fields(d)

    return d, path


def augmentation(d, name, type_options=None, out_fks=None):
    """
    - lookup options and traverse/apply
    - terate over a dictionary, track if it has nested dicts
    - pass the root cascade and root key down
    - pass the parent key down to make the edges - needed?e33
    """

    return traverse_dict_apply(
        d, name, action=action, type_options=type_options, out_fks=out_fks
    )


def mutate(q):
    r = safe_http.request_post(
        f"{DGRAPH}/mutate?commitNow=true&upsert=true",
        data=json.dumps(q),
        headers={"Content-Type": "application/json"},
        timeout=MUTATION_TIMEOUT,
    )
    return r


def quote_str(s):
    if isinstance(s, str):
        s = f""" "{s}" """
    # other types?
    return s


def apply_schema(dql_schema):
    """
    this applies dql schema
    """
    options = "?runInBackground=true"
    r = safe_http.request_post(f"{DGRAPH}/alter{options}", data=dql_schema)
    return r.json()


def key_resolver(table, key):
    """
    table: the entity or table name (node) :> in the query we add the dgraph type and all that
    key: the xid key column

    this is to create nodes in the graph safely. We want to always create a placeholder by xid and we can add to it later
    this can be done in batch queries but its useful to create an independant proces to manage keys in batch e.g. using redis or something

    note we define these as lookup types
    """
    query = f"r(func: type({table})) @filter(eq(xid, {quote_str(key)})) {{ root as uid, airtable.rid}} "
    response = mutate(
        {
            "query": f"{{ {query} }}",
            "set": [
                {
                    "uid": "uid(root)",
                    "dgraph.type": [table, "lookup_type"],
                    "xid": key,
                    "key": key,
                }
            ],
        }
    )

    response = response.json()

    # if its a new thing, we will get it back
    try_new = response["data"].get("uids", {}).get("uid(root)")
    if try_new:
        return try_new
    # otherwise we will resolve in the query
    return response["data"]["queries"]["r"][0]["uid"]


def explain_response_single(r):
    """
    using convnetions for how we write mutations in our resGraph, explain ther response
    we get back dgraph and airtable ids from upserts

    we can add statsd to this response to track all sorts of useful things
    """
    if r.status_code != 200:
        raise Exception(f"Failed to make request to Dgraph - TODO explain")
    r = r.json()
    errors = r.get("errors")
    if errors:
        raise Exception(str(errors))
    try_new = r["data"].get("uids", {}).get("uid(root)")
    if try_new:
        return {"uid": try_new, "mode": "create"}
    # otherwise we will resolve in the query by name of query -> conventions used for now
    # the root node query for upsert
    root_query = r["data"]["queries"].get("r")
    # the nested children cascades - this works because when we upsert we augment valid child nodes with a root id - the cascade is an interface type e.g. parent_type_cascade
    # for example if we had order items under order, the order items would implement the interface `order_cascade` with the roots id. this way we can purge all of the tree if we want to
    cascades = r["data"]["queries"].get("c")
    # dgraph gives us back any new nodes that are created - be sure we intended to create new nodes and not orphans
    new_uids = r["data"].get("uids")
    # package up the response
    if root_query and len(root_query):
        first = root_query[0]
        first["mode"] = "update"
        if cascades:
            first["cascaded_uids"] = [c["uid"] for c in cascades]
        if new_uids:
            first["new_nodes"] = new_uids
        return first
    return None


def inversion_kwargs_from_avro_type(avro_type_key, graph_config, select_field="alias"):
    """
    take what would be a
     upsert(event, 'printed_rolls', graph_config=g)
    and do a parent->child, parent field
     upsert_with_parent_child_inversion(event, 'printed_rolls', 'piece_info' , graph_config=g)

     Returns:
        "name" : parent['alias'],
        "child_name" : child['alias'],
        "child_field" : field_on_parent.get('name')

    """

    c = graph_config[avro_type_key]

    refs = c.get("referenced_by_type", [])
    if len(refs) != 1:
        raise NotImplementedError(
            f"Unable to find inversion args for types {avro_type_key} with multiple parent or other references: {refs}"
        )

    p = graph_config[refs[0]]

    field = {}
    for f in p["fields"]:
        if isinstance(f["type"], dict):
            field = f["type"].get("items")
            if field == avro_type_key:
                field = f

    return {
        # it would seem like schema key is correct but
        "parent_name": p[select_field],
        "child_name": c[select_field],
        "child_field": field.get("name"),
    }


def upsert_primitive(asset, name, graph_config=None, plan=False, **kwargs):

    if kwargs.get("add_primitive_hash"):
        asset = add_hash_for_fields(asset)

    return upsert(
        asset=asset,
        name=name,
        graph_config=graph_config,
        only_primitives=True,
        plan=plan,
        **kwargs,
    )


def upsert(asset, name, graph_config=None, only_primitives=False, plan=False, **kwargs):
    """

    name: name is the alias name i.e. the dgraph name but at this point the asset can be mapped to a topic uniquely i.e. many topic keys could map to one alias

    example adding uid relations - dgraph will fail if you try to add strings into edges

    graph config is a dict but there is a generator or GraphModel for it which should be used

    """

    # if we are inverting the parent child, we can

    mutation = get_mutation_for(
        asset, name, graph_config, only_primitives=only_primitives, plan=plan, **kwargs
    )
    if plan:
        return mutation

    response = mutate(mutation)

    # this is the core upsert function that all others use we can
    if kwargs.get("propagate_new_xids", True):
        _ = _upsert_keys_in_new_node_responses(response)

    return explain_response_single(response)


def select_subgraph(g, name):
    """
    a sub graph should have unique aliases!
    We can select and rewrite them
    """
    return {v["alias"]: v for _, v in g.items() if v["name"] == name}


def upsert_with_parent_child_inversion(
    asset, parent_name, child_field, child_name, graph_config=None, plan=None
):
    """
    simply unpack the child

    example query if we start the edge at the child

    query{
        g(func: has(~edge_printed_rolls))
        {
            expand(_all_){  }
            ~edge_printed_rolls
            {
            expand(_all_){}
            }
        }
    }

    """
    responses = []
    asset = dict(asset)
    child_assets = asset.pop(child_field)

    responses.append(upsert(asset, parent_name, graph_config=graph_config, plan=plan))

    for child in child_assets:
        # its assumed that we add a parent child relationship to the model
        # add an fk code as we normally would for the parent if it does not exist
        key_val = quote_str(get_asset_key(asset, parent_name, graph_config))
        parent_query = f"my_parent(func: type({parent_name})) @filter(eq(xid, {key_val})) {{ parent as uid}}\n"
        child[f"edge_{parent_name}"] = "uid(parent)"
        responses.append(
            upsert(
                child,
                child_name,
                graph_config=graph_config,
                plan=plan,
                custom_queries=parent_query,
            )
        )

    return responses


def _upsert_keys_in_new_node_responses(responses):
    """
    'new_nodes': {'uid(fk_roll_piece_assets_asset_id_TO_make_piece_assets_recTr7PBK8liapBTR)': '0x1ad3d1'}}]
    """
    if not isinstance(responses, list):
        responses = [responses]
    counter = 0
    try:
        for r in responses:
            new_nodes = r.json().get("new_nodes", {})
            for k, uid in new_nodes.items():
                parts = k.rstrip(")").split("_")
                type = parts[-2]
                xid = parts[-1]

                add_xid_for_uid(type, xid, uid)
                counter += 1
    except Exception as ex:
        res.utils.logger.warn(
            f"Failed to do the lookup resolution - this can be cleaned up later so simply warning: {res.utils.ex_repr(ex)}"
        )
    return counter


def add_xid_for_uid(type, xid, uid):
    """
    find_using_uid(func: uid(UID)){
        uid
    """
    mutation = {
        "query": f"f(func:(uid({{{uid}}}){{u as uid}}",
        "set": {"uid": "uid(u)", "xid": xid},
    }

    return mutate(mutation)


def lookup_type_by_key(name, key, **kwargs):
    sys_fields = f"\n".join(SYS_FIELDS)
    q = f"""query{{
        get(func: type({name}))
        @filter(eq(xid,"{key}"))
        {{
        {sys_fields}
        expand(_all_){{ 
            expand(_all_){{}}
        }}
        }}
    }}"""

    return query(q, **kwargs)


def query(q, hide_meta_data=True, **kwargs):
    response = safe_http.request_post(
        f"{DGRAPH}/query",
        data=q,
        headers={"Content-Type": "application/dql"},
        timeout=kwargs.get("timeout", QUERY_TIMEOUT),
    )
    # do some checks that we successfully got an item
    result = response.json()

    if hide_meta_data:
        return result["data"]

    return result


def query_type(name, config=None, **kwargs):
    """
    just for looking up the type
    we can do some config manip maybe later e.g. reverse edges etc.

    sys fields should never be on the schema!!!!
    """
    sys_fields = f"\n".join(SYS_FIELDS)
    q = f"""query{{
        get(func: type({name}))
        {{
        {sys_fields}
        expand(_all_){{ 
            expand(_all_){{}}
        }}
        }}
    }}"""

    return query(q, **kwargs).get("get", [])


def get_changes(
    name, modified_after=None, expanded=False, exclude_airtable_purged=True
):
    """ """

    if modified_after is None:
        modified_after = "2020-01-01"

    filters = f"""@filter(ge(sys.modified, "{modified_after}") OR ge(airtable.purge_at, "{modified_after}")) """

    if exclude_airtable_purged:
        filters = f"""@filter( (ge(sys.modified, "{modified_after}") OR ge(airtable.purge_at, "{modified_after}")) AND not has(airtable.purge_confirmed_at)) """

    q = f"""
        query{{ 
            get(func: type({name}))
            {filters}
            {{ 
                uid
                xid
                {"" if not expanded else "expand(_all_)"}
                sys.modified
                airtable.rid
                airtable.purge_at
                airtable.purge_confirmed_at
            }}
       }}
    """
    res.utils.logger.debug(q)
    return query(q, timeout=QUERY_TIMEOUT).get("get", [])


def get_mutation_for(
    d,
    name,
    type_options=None,
    augment=True,
    only_primitives=False,
    plan=False,
    **kwargs,
):
    """

    three things
    1. keep your own type information and pass it on the node tree
    2. add queries to upsert blocks for delete and uid resolution by convention
    3. augment the dictionary to have special ids and types used for getting data back out etc.

       a lookup cache could be used but we dont for now
    ................................................................
    the mutation tries to determine if we need a deep or shallow upsert
    complex types are lists of dicts or dicts and if we have them we always deep upsert because it seems cheeper than traversing edges
    the deep cascades are based on convention for inserts (this is a backend tool so we can enforce) where we add the keys we need on the tree/graph
    type options including keys and aliases are taken from a graph ql schema or stub that we use to generate the schema
    we use a lookup strategy for deep fks in the augmentation rather than querying as mostly we are running in a process that manages many things that can be cached

    it is important to note/test that type options is a map of options for multiple types to support hierarchy
    each function operations f(name, type_options) so it can reference into its own options if they are supplied
    """
    # actually the type options must be a map into
    options = (type_options or {}).get(name, {})

    key = options.get("key", "key")
    key_val = quote_str(d.get(key))
    # assertion on key val
    assert (
        key_val is not None
    ), f"Unable to resolve a key value for {name}.{key} - is the config correct?"
    query = [
        f"r(func: type({name})) @filter(eq(xid, {key_val})) {{ root as uid, airtable.rid, airtable.purge_at}}\n"
    ]

    delete_queries = []
    ctypes = list_complex_types(d)  # filter {upserts: by value}

    # by_ref_upserts = {}

    if ctypes and not only_primitives:
        query += [
            f"c(func: type({name}_cascade)) @filter(eq(cascade.xid, {key_val})){{cascaded_objects as uid}}\n"
        ]
        # if there are complex child types this seems to be how we would purge both edges and edge targets
        delete_queries = [{"uid": "uid(root)", k: None} for k in ctypes] + [
            {"uid": "uid(cascaded_objects)"}
        ]

    # this is a map because they must be unique - look for conflicts
    out_fks = {}
    if augment:
        # pass in a collector for the deep fks and their unique names tree_entity__field
        # passing this down is better because we can check if the fields are none null before suggesting lookups rather than trust the schema
        d = augmentation(d, name, type_options, out_fks)
        for fk_tuple, fk_value in out_fks.items():
            fk, fk_type = fk_tuple
            # the function is a typed pointer to a value e.g. func_product_categories_ABC -> xid == 'AVC -> uid()
            query += [
                f"func_{fk}(func: type({fk_type})) @filter(eq(xid, { quote_str(fk_value)})){{fk_{fk} as uid }}\n"
            ]

    # we can pass in custom queries from other flows but it is assumed we can resolve them
    # for example when we are inverting we need to create a mutation batch and it does not make much sense to do that in here
    custom_queries = kwargs.get("custom_queries", [])
    if not isinstance(custom_queries, list):
        custom_queries = [custom_queries]
    query += custom_queries

    if not plan:
        query = " ".join(query)
        query = f"{{{query}}}"
    return {"query": query, "delete": delete_queries, "set": [d]}


def _airtable_from_config(config, create=True):
    from res.connectors.airtable.AirtableConnector import AIRTABLE_ONE_PLATFORM_BASE

    name = titlecase(config._alias)
    airtable = res.connectors.load("airtable")
    b = airtable[AIRTABLE_ONE_PLATFORM_BASE]
    return b[name]


def sync_airtable_rids(graph, name, date=None):
    """
    in the case we do not have airtable ids we can fetch them - this is usually a full table load
    assume that airtable has already been populated by dgraph in the first place but for example shit happens and also we may want to load between envs
    """
    config = graph._loaded_topics[name]
    key_field = config._key_field
    res.utils.logger.debug(f"Syncing {config._alias} for key field {key_field}")
    res.utils.logger.debug(f"getting dgraph changes on {config._alias}")
    data_xids = get_changes(config._alias, modified_after=date)
    data_xids = pd.DataFrame(data_xids)

    atable = _airtable_from_config(config)
    fields = [
        f
        for f in ["Status", "Tags", "Notes", "sys.primitive_hash"]
        if atable.has_field(f)
    ]
    res.utils.logger.debug(
        f"Looking up keys with fields {fields} for table modified after {date}"
    )
    adata = atable.key_lookup(keys=None, fields=fields, user_modified_at=date)
    res.utils.logger.debug(f"Found {len(adata)} records")

    if len(adata):
        keys = dict(adata[[atable.key_field, "record_id"]].values)

    added_keys = {}

    # adding missing keys
    for record in data_xids[data_xids["airtable.rid"].isnull()].to_dict("records"):
        k = record["xid"]
        added_keys[k] = False
        if k in keys:
            update = {
                # we can use the xid from the changes lookup at this level
                key_field: k,
                "airtable.rid": keys.get(k),
            }
            res.utils.logger.debug(f"adding {update}")
            upsert_primitive(update, config._alias, graph_config=graph)
            added_keys[k] = True

    res.utils.logger.info(f"synced airtable rids")

    return added_keys


def get_dgraph_data_and_rename(graph, name, data, raw=False):
    config = graph._loaded_topics[name]
    key_field = config._key_field
    akey = config._airtable_key or key_field
    alias = config._alias

    dgraph_data = (
        data
        if data is not None
        # if we do this we should make sure to pull the system fields
        else graph.query_type_by_alias(alias, timeout=60)
    )
    df = pd.DataFrame(dgraph_data)

    if raw:
        return df

    df.columns = [
        titlecase(t) if "." not in t and t not in key_field else t for t in df.columns
    ]
    # rename for airtable - xid -> akey and drop the key field above
    df = df[[c for c in df.columns if c != titlecase(akey)]].rename(
        columns={"airtable.rid": "record_id", "Xid": titlecase(akey)}
    )
    airtable_fields = [f for f in df.columns if f not in ["Uid", "Metadata"]]
    # attachment_fields = [a["name"] for a in config.airtable_attachments]

    for c in SYS_FIELDS:
        if c not in df.columns:
            df[c] = None

    df = df[[f for f in df.columns if f in airtable_fields]]

    # remove NaN types
    df = df.where(pd.notnull(df), None)
    if "record_id" not in df.columns:
        df["record_id"] = None

    return df


########                                                                                                         #######
###           FUNCTIONS BELOW HERE ARE REALLY AIRTABLE RELATED AND CAN BE MOVED WHEN WE FLESH OUT THE PATTERN        ###
########                                                                                                         #######

# do a batch merge transfer here...
def _pull_record_from_remote_batch(record, remote_batch, airtable_owned_fields):
    """
    check if the remote is different
    if its not return None to let the caller know there are no changes
    otherwise apply the remote changes from the owned fields for update
    """

    remote_rec = dict(remote_batch.set_index("record_id").loc[record["record_id"]])

    # print("hash", remote_rec["sys.primitive_hash"], record["sys.primitive_hash"])
    if remote_rec["sys.primitive_hash"] == record["sys.primitive_hash"]:
        res.utils.logger.debug(
            f"no primitive change - return empty and suggest continue"
        )
        # there is nothing to change - return empty
        return None

    for k in airtable_owned_fields:
        if k in remote_rec:
            record[k] = remote_rec[k]

    return record


def _add_attachments(record, config):
    """
    we replace airtable attachment fields with links to the actual attachments just prior to saving
    """
    att = config.attachment_iterator_for_asset(record, case_function=titlecase)
    for k, v in att.items():
        if titlecase(k) in record:
            record[titlecase(k)] = [{"url": item} for item in v if item is not None]
            if len(record[titlecase(k)]) == 0:
                record[titlecase(k)] = None
    return record


def purge_missing_ids(alias, key, rids, graph):
    """
    the airtable should actually be the title case of the alias
    we remove fields in database that are not in airtable using the xid mapped to the current key

    r = tab.key_lookup()
    rids = list(r['record_id'])

    purge_missing_ids(dgraph_table_name, key_field_for_xid, rids_that_exist_in_airtable, the_graph_config_object)
    """
    data = query_type(alias)
    data = pd.DataFrame(data)

    # this is what we want to check - a non null airtable rid that is no in airtable but has not been purged
    chk = data[~data["airtable.rid"].isin(rids)]
    chk = chk[chk["airtable.purge_confirmed_at"].isnull()]
    chk = chk[~chk["airtable.rid"].isnull()]

    results = []
    from tqdm import tqdm

    for record in tqdm(chk.to_dict("records")):
        aid = record.get("airtable.rid")
        try:
            if aid and aid not in rids and pd.notnull(aid):

                record = {
                    # using the xid is def safe but original keys may have evolved in some way we dont know
                    key: record["xid"],
                    "airtable.purge_confirmed_at": res.utils.dates.utc_now_iso_string(
                        None
                    ),
                }
                # print(record)
                result = upsert_primitive(record, name=alias, graph_config=graph)
                results.append(result)
        except Exception as ex:
            print(record)
            print("failed one", ex)
            # raise ex

    return results


def sync_airtable_data(
    graph, name, data, plan=False, check_remote_first=True, write_back_changes=True
):
    """

    e.g entity = 'printfile_events'
    we assume the table exists at this point

    testing: validate that we have all the right fields and right types
    #metadata was not added why nd float values were incorrect
    """

    config = graph._loaded_topics[name]
    df = get_dgraph_data_and_rename(graph, name, data=data)
    res.utils.logger.debug(
        f"loaded {len(df)} records for name {name} in total - columns are {list(df.columns)}"
    )

    if len(df) == 0:
        return pd.DataFrame()
    # need for updates
    gc = select_subgraph(graph, config._topic_name)
    name = titlecase(config._alias)
    res.utils.logger.info(f"sync table {name} in env {RES_ENV}")
    atable = _airtable_from_config(config)
    key_field = config._key_field
    results = []
    akey = config._airtable_key or key_field
    ################################### refactor - loading existing in the right format
    # todo we can later query in a more efficient way - timeout for now for testing bulk loads

    airtable_owned_fields = [
        f
        for f in ["Status", "Tags", "Notes", "sys.primitive_hash"]
        if f in list(atable.fields["name"])
    ]
    #####################################

    #################################### state check
    # fetch remote changes on airtable owned fields
    if check_remote_first:
        res.utils.logger.info("getting existing records that do need to be purged")
        # we need to somehow get all the remote status fields for things that are created already and not deleted
        remote_batch = atable.key_lookup(fields=airtable_owned_fields)
        remote_batch = remote_batch.where(pd.notnull(remote_batch), None)
        remote_ids = (
            list(remote_batch["record_id"].unique()) if len(remote_batch) else []
        )
        res.utils.logger.info(
            f"remote id count {len(remote_ids)} - will purge any stale ones"
        )

        res.utils.logger.debug(
            "removing any ids that are not on the remote because they are invalid"
        )
        df["record_id"] = df["record_id"].map(
            lambda a: a if len(remote_ids) and a in remote_ids else None
        )

        res.utils.logger.debug(
            f"we have {len(df[df['record_id'].notnull()])} record ids that are on the remote"
        )

    ##############updates############
    res.utils.logger.debug(
        f"Updating {len(df)} records in total using our key {key_field}/{akey} - columns are {list(df.columns)}"
    )

    # group by mode and then do a batch update on airtable in future
    for record in df.to_dict("records"):

        # we may have void op
        result = None

        if record.get("airtable.purge_at") and record.get("record_id"):
            if not plan:
                result = atable.delete_with_retries(record)
                # set the confirmation
                result[
                    "airtable.purge_confirmed_at"
                ] = res.utils.dates.utc_now_iso_string(None)
        else:
            # at this point we should drop record ids having checked teh server if they are stale ids: remote rules
            mode = "create" if not record.get("record_id") else "update"
            if mode != "create" and check_remote_first:
                record = _pull_record_from_remote_batch(
                    record,
                    remote_batch=remote_batch,
                    airtable_owned_fields=airtable_owned_fields,
                )
                if not record:
                    continue
            if not plan:
                # there are cases here where we think we have a record id but someone deletes it - the client should just handle this case and push with new record id and make mode create
                # we should also checkout sys hash because we may not need to update anything ?
                record = _add_attachments(record, config)
                # this is an incomplete abstraction - we do not explicitly model column mappings yet

                if titlecase(akey) not in record:
                    raise Exception(
                        f"invalid configuration - the key {titlecase(akey)} is not in {record}"
                    )

                result = atable.update_with_retries(record)
            # update the sys primitive hash and select only the airtable owned fields in camel casing
            # this only needs to be a pull with metadata

            if write_back_changes:
                updated_record = {
                    camelcase(k): record[k]
                    for k in (set(airtable_owned_fields) & set(record.keys()))
                }
                updated_record["airtable.rid"] = result["id"]
                updated_record["airtable.purge_at"] = None
                # put the key field back
                updated_record[key_field] = record[titlecase(akey)]
                # we need to add the updates from airtable
                # - if the mode is created we have the new record id and generally we have the airtable owned states in update mode
                if not updated_record.get(key_field):
                    res.utils.logger.warn(
                        f"Found a record with no key {updated_record}"
                    )
                if not plan and updated_record.get(key_field):
                    # print(updated_record)
                    # we could check if the updated
                    upsert_primitive(
                        updated_record,
                        config._alias,
                        graph_config=gc,
                        add_primitive_hash=True,
                    )

        if result:
            results.append(result)
    # add the initial lookup latency here to results on the batch
    return pd.DataFrame(results)


"""
Test helpers
"""

from res.media.images.geometry import Point

# this is a nest structure
EXAMPLE_PAYLOAD = {
    "name": "Test Parent A1",
    # pattern 1: if implicit_foreign_type is defined as an entities, we will try to add the edge as rel_entity_type to expand in graphQL queries
    "implicit_foreign_type": "ift_code",
    "key": "test_root",
    # tags, statas, notes are synced with airtable and we use tag manager to notify owners etc.
    "tags": ["A", "B", "C", "D"],
    "status": "TODO",
    "notes": "text",
    # todo - geometry
    "created_at": utc_now_iso_string(None),
    # pattern2: children are purged and updated so we dont need any smarter way to manage relationships (just dont leave orphans)
    "children": [
        {
            "name": "Test Child A1",
            "key": "testa",
            "created_at": utc_now_iso_string(None),
        },
        {
            "name": "Test Child A2",
            "key": "testb",
            "created_at": utc_now_iso_string(None),
            "grand_child": {"key": "testgc", "name": "test"},
        },
    ],
    # dicts are mapped to json strings ny default
    "metadata": {"setting": "value"},
    # todo geometry type e.g. a point can be serialized and de-serialized as strings or binary - here point is already string and its fine
    "point_geometry": "POINT(0,0)",
    # - in this case the point is an object and we need to-string it
    "point_geometry": Point(1, 1),
    # todo pandas timestamps that show the need for custom serilizing of these fields
}

EXAMPLE_PAYLOAD_WITH_ADDED_EDGE = dict(EXAMPLE_PAYLOAD)
# you can register the test body if it does not exist with key_resolver('bodies', "TEST_BODY")
# this shows that bodies is a type that we try to add a link to because its a known entity
EXAMPLE_PAYLOAD.update({"body_code": "TEST_BODY"})
