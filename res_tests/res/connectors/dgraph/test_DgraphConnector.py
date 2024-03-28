import res

import yaml
import pytest


@pytest.mark.service
def test_loads_connector():
    connector = res.connectors.load("dgraph")
    assert connector is not None, "connector could not be loaded"


@pytest.mark.service
def test_loads_meta_schema():
    from res.connectors.dgraph import DgraphConnector
    from res.connectors.dgraph.DgraphConnector import META_ENTITY_DGRAPH_NODE_NAME

    meta = DgraphConnector.try_load_meta_schema(META_ENTITY_DGRAPH_NODE_NAME)

    assert (
        meta.get("key") == META_ENTITY_DGRAPH_NODE_NAME
    ), f"the dgraph node name is not {META_ENTITY_DGRAPH_NODE_NAME}"

    assert meta.get("fields"), "could not load fields from metadata"

    child_types = [
        f["name"] for f in meta.get("fields", []) if f.get("is_child_type", False)
    ]

    assert (
        len(child_types) > 0
    ), "expected a child type called fields in the meta.entities defintion"


@pytest.mark.service
def test_update_meta_record_with_child_types():
    spec = None

    # documenting what is needed in the schema -> schema in registry must load child types
    # res.connectors.load(['dgraph'])['meta.entities'].update_record(spec, schema={"child_types": {'fields': 'meta.fields'}})
    # but if the schema is defined we do not need to specify these details
    # meta records do not actually refer to real types and this is just config
    # for example if a type refers to some type like "bodies" or "styles" those types do not need to exist\
    # because the meta data is just string configs. We could valid
    res.connectors.load(["dgraph"])["meta.entities"].update_record(spec)


@pytest.mark.service
def test_update_meta_record_with_fk_types():
    spec = None

    # see comments in `test_update_meta_record_with_child_types` which apply also here
    res.connectors.load(["dgraph"])["meta.entities"].update_record(spec)


@pytest.mark.service
def test_apply_meta_schema_conventions():
    # keys and names on fields
    # etc.
    assert 1 > 0, "nope"


@pytest.mark.service
def test_makes_dql_from_res_meta():
    """
    This test is a basic load test and illustrates how res.meta schema are used to create dql schema
    these are applied to dgraph using the connectors alter schema feature
    """

    from res.connectors.dgraph import DgraphConnector

    res_spec = list(
        yaml.safe_load_all(
            open("res_tests/.sample_test_data/res_meta_schema/meta.entities.yaml")
        )
    )

    # this operatoes on one or list of specs

    s = DgraphConnector.make_dql_schema_from_meta_type(res_spec)

    assert "type meta.fields" in s, "type meta.fields not found in the dql schema def"
    assert (
        "type meta.entities" in s
    ), "type meta.entities not found in the dql schema def"

    # DgraphConnector.alter_schema(s)


@pytest.mark.service
def test_updates_registry_from_meta_types():
    res_spec = list(
        yaml.safe_load_all(
            open("res_tests/.sample_test_data/res_meta_schema/meta.entities.yaml")
        )
    )

    # we do not need to save the meta payload as we just generate it in code but...
    for s in res_spec:
        key = s["key"]
        # why do we not need to insert fields?
        if key == "meta.entities":
            res.connectors.load("dgraph")[key].update_record(s)


def test_handle_list_fields_no_meta():
    pass


def test_handle_list_fields_meta():
    pass


def test_handle_resolve_edge_single():
    pass


def test_handle_resolve_edge_multiple():
    pass


# test mutation for cols missing wrt schema - > should fail silently when fields are not required
# test dont add index to complex types
# test saves and reloads interfaces on dgraph types
# test preserves simple list types


# import res
# import yaml
# from res.connectors.dgraph import DgraphConnector
# from res.connectors.airtable import AirtableWebhookSpec
# dgraph = res.connectors.load('dgraph')
# metatype =    "make.nest_assets"
# meta_spec = "../../res-schemas/res/make/nest_assets.yaml"
# s = res.utils.read(meta_spec)
# DgraphConnector.update_type_from_metatype(metatype, s)
# dgraph.try_load_meta_schema('make.nest_assets')
# mut = dgraph.create_update_mutation_for_type(record=test, name=metatype, key='key')
# mut

# from res.utils import safe_http
# from res.connectors.dgraph.DgraphConnector import DGRAPH
# res = safe_http.request_post(  f"{DGRAPH}/mutate?commitNow=true", json=mut      )
# res.json()
