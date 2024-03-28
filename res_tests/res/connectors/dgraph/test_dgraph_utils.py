import pytest

try:
    from res.connectors.dgraph.utils import *
    from res.connectors.dgraph.GraphModel import GraphModel
except:
    print("failed to import dgraph.utils in test")


@pytest.mark.service
def test_primitive_upsert():

    from res.utils import res_hash

    xid = res_hash()
    pp_data = {
        "dgraph.type": "print_assets",
        "id": xid,
        "body_code": "test_body",
        "body_version": "9",
        "sales_channel": "ECOM",
    }

    config = {
        "print_assets": {
            "key": "id",
            "name": "print_assets",
            "fks": GraphModel.ASSUMED_ENTITIES,
        }
    }
    result = upsert_primitive(
        pp_data, "print_assets", config=config, uid_lookups=DCache(), plan=False
    )

    assert result.get("mode") == "create"

    result = upsert_primitive(
        pp_data, "print_assets", config=config, uid_lookups=DCache(), plan=False
    )

    assert result.get("mode") == "update"


@pytest.mark.service
def test_upsert():
    """
    if unplanned we would qwuery with

    query{
        g(func: type(printed_rolls))
        @filter(eq(xid,"TESTKEY"))
        {
            uid
            job_key
            piece_info{
            uid
            asset_key
            asset_id
            edge_asset_id{
                uid
                xid
                }
            }
        }
    }
    """
    from .test_GraphModel import spec

    pfile_pieces = {
        "record_id": "recllH34TDwWYL43H",
        "task_key": "unkn",
        "job_keys": ["OTHERKEYS"],
        "stitching_job_key": "TESTKEY",
        "piece_info": [
            {
                "asset_id": "recTr7PBK8liapBTR",
                "asset_key": "10252215",
                "piece_name": "5",
            }
        ],
    }
    g = GraphModel(spec)
    query_planned = upsert(pfile_pieces, "printed_rolls", graph_config=g, plan=True)

    assert query_planned is not None, "failed a basic test to upsert"


"""
lookup_type_by_key('print_assets', 'recAVZqb5CLegTqvl-new',hide_meta_data=True) 
"""
