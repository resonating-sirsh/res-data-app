from res.flows import FlowContext, flow_node_attributes
from . import queries, helpers

"""
Add logic for reading style data and different assets as per notebook
Run the update to update hasura  
"""


@flow_node_attributes(
    memory="2Gi",
)
def handler(event, context=None):
    """
    <flow structure>
       args: plan
       #assets are a request for style updates - it is good practice today to make requests for a particular version of the body
       assets: [
         {
            "key" : SKU,
            "body_version" ?
         }
       ]


    """
    with FlowContext(event, context) as fc:
        plan = fc.args.get("plan", False)
        assets = fc.assets_dataframe
        skus = list(assets["key"].unique())
        # we can have actions and reasons in the request but for now we dont care and just process unique skus for any reason e.g. new style, material swaps etc.
        styles = helpers.process_styles(skus)
        if not plan:
            queries.update_styles(styles)
        else:
            return styles

    return {}


def generator(event, context=None):
    """
    all kafka requests will be collected here

    do a softish load for styles and keys in the generator to create the payloads for the helpers
    in the helpers we will do the full processing of the style and maybe even try the artwork
    would be good not to expand by size and then to the expansion in the handler
    """
    with FlowContext(event, context):
        pass

    return {}


def reducer(event, context=None):
    return {}
