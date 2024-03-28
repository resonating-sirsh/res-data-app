"""
This is assumed temporary - used forlegacy lookups
We combine snowflake and airtable data but this should just be in a proper database
"""

import res
import pandas as pd
from tenacity import retry, wait_fixed, stop_after_attempt


@retry(wait=wait_fixed(3), stop=stop_after_attempt(1), reraise=True)
def try_get_cancelled_one_number(id):
    GET_PRODUCTION_REQUEST_QUERY = """
       query getMakeOneProductionRequest($id: ID!){
            makeOneProductionRequest(id:$id){
                id  
                name
                orderNumber
                sku
                type
                allocationStatus
                currentAssemblyNode
                currentMakeNode
                sewResourceName
                sewResource
                requestName
                cutResourceName
                cancelLink
                salesChannel
                manualOverride
                cancelReasonTags
                originalOrderPlacedAt
                countReproduce
                activeIssueId
                isRequestPendingAssets
                flagForReviewTags
                reproducedOnesIds
                }
      }
    """

    try:
        graph = res.connectors.load("graphql")
        graph
        d = graph.query_with_kwargs(GET_PRODUCTION_REQUEST_QUERY, id=id)
        return d["data"]["makeOneProductionRequest"]["orderNumber"]
    except:
        return None


def get_airtable_production_orders(keys=None):
    """
    only
        get_airtable_production_orders(keys=['10219369'])

    """
    from res.connectors.airtable import AirtableConnector

    airtable = res.connectors.load("airtable")
    if keys:
        pred = AirtableConnector.make_key_lookup_predicate(keys, "Order Number v3")

        res.utils.logger.debug(f"Using predicate {pred} for airtable lookup")

        aprod_orders = airtable.get_airtable_table_for_schema_by_name(
            "make.production_requests",
            filters=pred,
        )
    else:
        aprod_orders = airtable.get_airtable_table_for_schema_by_name(
            "make.production_requests",
        )

    aprod_orders["material_code"] = aprod_orders["sku"].map(
        lambda x: x.split(" ")[1].lstrip().rstrip()
    )
    aprod_orders["color_code"] = aprod_orders["sku"].map(
        lambda x: x.split(" ")[2].lstrip().rstrip()
    )
    aprod_orders = aprod_orders.rename(columns={"key": "one_number"})

    return aprod_orders


def get_snowflake_production_orders():
    snowflake = res.connectors.load("snowflake")
    QUERY = f"""SELECT "Factory Request Name", "Current Assembly Node", "Current Make Node", "CUT Date", "Prep Exit Date", "Sew Date", "__cut_exit_at", "Paper Marker Exit Date", "Fabric Print Date", "Order Number v3", "_cancel_date", "__brandcode", "Style Code", "Size Code"
    FROM IAMCURIOUS_SCHEMA."make_one_production_request" 

    """
    data = snowflake.execute(QUERY)

    def make_sku(row):
        return f"{row['Style Code']} {row['Size Code']}".replace("-", "")

    chk = data[["Order Number v3", "Style Code", "Size Code"]].dropna()
    chk["body_code"] = chk["Style Code"].map(
        lambda x: x.split(" ")[0].lstrip().rstrip()
    )
    chk["material_code"] = chk["Style Code"].map(
        lambda x: x.split(" ")[1].lstrip().rstrip()
    )
    chk["color_code"] = chk["Style Code"].map(
        lambda x: x.split(" ")[2].lstrip().rstrip()
    )
    chk["sku"] = chk.apply(make_sku, axis=1)

    chk = chk.rename(
        columns={
            "Order Number v3": "one_number",
            "Style Code": "style_code",
            "Size Code": "size_code",
        }
    )

    return chk


def get_production_record(key, warehouse_store="test_production_requests"):
    from res.connectors.snowflake import CachedWarehouse
    from res.flows.make.production_requests import get_airtable_production_orders

    try:
        lookup = CachedWarehouse(
            warehouse_store,
            key="one_number",
            missing_data_resolver=get_airtable_production_orders,
        )
        record = dict(lookup.lookup_keys(key).iloc[0])
        return record
    except Exception as ex:
        res.utils.logger.warn(
            f"Failed to get production request for key {key}: {repr(ex)}"
        )


def get_all_production_orders(restrict_cols=True):
    """
    This can be saved to a warehouse cache

        CachedWarehouse('production_requests',df=data, key='one_number').write()

        We can construct the warehouse with the missing_key_resolver=get_airtable_production_orders

    Then we can load from the cache in future
    """
    sdata = get_snowflake_production_orders()
    adata = get_airtable_production_orders()
    # restrict to what we should know about
    if restrict_cols:
        adata = adata[sdata.columns]
    data = (
        pd.concat([adata, sdata])
        .drop_duplicates(subset=["one_number"])
        .reset_index()
        .drop("index", 1)
    )
    return data
