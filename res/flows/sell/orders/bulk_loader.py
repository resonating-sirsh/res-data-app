import res


def generator(event, context={}):
    with res.flows.FlowContext(event, context) as fc:
        page_size = fc.args["page_size"]
        num_pages = fc.args["pages"]
        offset = fc.args.get("offset")

        assets = [
            {"page": i, "page_size": page_size, "offset": offset}
            for i in range(num_pages)
        ]

        assets = fc.asset_list_to_payload(assets)
        return assets


@res.flows.flow_node_attributes(
    memory="4Gi",
)
def handler(event, context={}):
    from res.flows.sell.orders.queries import bulk_load_from_fulfillments_warehouse

    with res.flows.FlowContext(event, context) as fc:
        for asset in fc.assets:
            page_size = asset["page_size"]
            offset = asset["offset"]
            page = asset["page"]
            load_airtable = fc.args.get("load_airtable", False)

            bulk_load_from_fulfillments_warehouse(
                None,
                page=page,
                page_size=page_size,
                offset=offset,
                load_airtable=load_airtable,
            )
