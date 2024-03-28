try:
    from res.connectors.dgraph.GraphModel import GraphModel

except:
    pass
import pytest

# res-meta/dxa/prep_pieces_responses.avsc.untracked
spec = """
topics:
  - name: res_make.optimus.printfile_pieces
    schema_path: res-make/optimus/printfile_pieces.avsc
    schema_key: PrintfilePieceInfo
    alias: make_piece_assets
    relationships:
    - name: asset_id
      entity_type: make_piece_assets
      cardinality: 1
    key: piece_id
    airtable:
      name: Make Piece Assets
      additional_two_way_bindings: []
  - name: res_make.optimus.printfile_pieces
    schema_path: res-make/optimus/printfile_pieces.avsc
    schema_key: PrintfilePieces
    alias: printed_rolls
    key: stitching_job_key
    airtable:
      name: Printed Rolls
      additional_two_way_bindings: []
"""


@pytest.mark.service
def test_graph_init():
    g = GraphModel(spec)
    aliases = g._shared_aliases

    expected = {
        "PrintfilePieceInfo": "make_piece_assets",
        "PrintfilePieces": "printed_rolls",
    }

    assert set(aliases.keys()) == set(expected.keys()), "shared aliases not created"

    for k, v in aliases.items():
        assert v == expected[k], "The alias mapping is not correct"

    lookup1 = g.get("printed_rolls")
    lookup2 = g.get("make_piece_assets")
    assert lookup1 is not None and lookup2 is not None, "we could not load a type"


@pytest.mark.service
def test_makes_schema():
    g = GraphModel(spec)
    s = g.as_dql_schema()

    assert s is not None


@pytest.mark.service
def test_alias_child_type():
    """
    this is a subtle relationship because we need to look at the property on a parent -> check its avro type -> and then map this in our config to our own type
    """
    g = GraphModel(spec)
    alias = GraphModel.alias_of_type("printed_rolls", "piece_info", g)

    assert (
        alias == "make_piece_assets"
    ), f"The alias is not correct - expected make_piece_assets and got {alias}"
