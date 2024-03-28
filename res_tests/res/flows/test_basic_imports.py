import pytest


@pytest.mark.service
def test_loads_nesting():
    from res.flows.make.nest.progressive import pack_one, construct_rolls

    loaded = [pack_one, construct_rolls]

    assert len(loaded) == 2, "could not load some nesting mods"


# this stuff is a good test but dont always have creds
@pytest.mark.service
def test_loads_connectors():
    import traceback
    import res

    missing = {}
    for c in [
        "box",
        "snowflake",
        "airtable",
        "s3",
        "shortcut",
        "shopify",
        "graphql",
        "hasura",
        "argo",
        "mongo",
    ]:  #'weaviate', 'looker', 'quickbooks
        try:
            res.connectors.load(c)
        except Exception as ex:
            missing[c] = traceback.format_exc()
            res.utils.logger.warn(f"Missing {c}")
    assert len(missing) == 0, "Could not load some connectors"
