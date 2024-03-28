import res
from res.learn.optimization.nest import nest_asset_set, evaluate_nesting
from res.flows.meta import bodies
import pandas as pd
from res.flows.etl.production_status import add_product_instance_info_for_one_numbers


def handler(event, context):
    """
    This is the simple flow that
    - maps one requests to body requests
    - assumes that all assets in the set are the same material

    If multiple materials are passed the material lookup with have more that one result and complain by default
    We augment style, body and material attributes from lookups

    We apply the nesting result to the asset data
    We create the print file and notify the callback and send data to kafka
    Printed assets stores the entire result that we evaluate with respect to the request
    """
    with res.flows.FlowContext(event, context) as fc:
        # TODO: check efficiency when we dont supply the nesting length as an input and just default to MAX size. I dont think it matters

        # TODO:options for what to do if one asset fails

        # this is messy right now - this should be a really simple query on a transactional database
        assets = add_product_instance_info_for_one_numbers(fc.assets_dataframe)
        # the outlines are read from the dxf and added to each piece - the piece is expanded using the fxf file
        assets = bodies.add_piece_outlines(assets)
        # we can optionally try and nest what we can if there are body requests for ones with inaccessible digital assets
        nested_assets = fc.apply("nest_asset_set", nest_asset_set, assets, key=fc.key)
        fc.apply("evaluate_nesting", evaluate_nesting, assets, key=fc.key)

        # when publishing nested assets compare with what was requested
        return fc.publish(nested_assets)


def on_failure(event, context):
    # find notification - load the error payload
    pass


def on_success(event, context):
    pass
