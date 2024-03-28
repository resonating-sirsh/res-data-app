import res
import pandas as pd


def get_orders_violating_id():
    QM0 = """query MyQuery {
    sell_orders {
        
        id
        deleted_at
        sales_channel
        order_channel
        source_order_id
        updated_at
    }
    }

    """
    hasura = res.connectors.load("hasura")
    odata = pd.DataFrame(hasura.execute_with_kwargs(QM0)["sell_orders"])
    odata.sort_values("updated_at")

    def make_order_id(values):
        return res.utils.uuid_str_from_dict(
            {
                "id": str(values["source_order_id"]),
                "order_channel": values["order_channel"],
            }
        )

    odata["eid"] = odata.apply(make_order_id, axis=1)
    res.utils.logger.info(f"testing orders: {len(odata)}")
    bad = odata[odata["id"] != odata["eid"]]
    res.utils.logger.info(f"testing orders not matching ids: {len(bad)}")
    bad = bad[bad["source_order_id"].notnull()]
    res.utils.logger.info(f"removing for missing source id: {len(bad)}")
    bad = bad[bad["source_order_id"].map(lambda x: "rec" not in x)]
    res.utils.logger.info(f"removing bad airtable source id: {len(bad)}")
    bad = bad[bad["deleted_at"].isnull()]
    res.utils.logger.info(f"removing marked for delete: {len(bad)}")

    return bad


def get_order_items_violating_id(make_orders_only=False):
    QM1 = """query MyQuery {
    sell_order_line_items(where: {order: {deleted_at: {_is_null: true}}}) {
        order_id
        id
        sku
        updated_at
    }
    }

    """

    hasura = res.connectors.load("hasura")

    def nid(values):
        return res.utils.uuid_str_from_dict(
            {"order_key": values["order_id"], "sku": values["sku"]}
        )

    oidata = pd.DataFrame(hasura.execute_with_kwargs(QM1)["sell_order_line_items"])
    oidata["nid"] = oidata.apply(nid, axis=1)
    res.utils.logger.info(
        f"testing orders: {len(oidata)} - filtering for those with wrong id"
    )
    changes = oidata[oidata["id"] != oidata["nid"]]

    if make_orders_only:
        res.utils.logger.info(
            (f"There were {len(changes)} changes - filtering for make orders")
        )
        Q = """query MyQuery {
            make_one_orders {
                id
                order_number
                one_number
                order_item_id
                sku
                order_item_rank
            }
            }
            """
        LU = pd.DataFrame(hasura.execute_with_kwargs(Q)["make_one_orders"])
        changes = pd.merge(
            changes,
            LU,
            left_on="id",
            right_on="order_item_id",
            suffixes=["", "_make_order"],
            how="left",
        ).dropna()

    return changes.sort_values("updated_at")
