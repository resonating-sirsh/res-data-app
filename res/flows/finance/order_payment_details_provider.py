import json

import pandas as pd

import res
import res.connectors


class OrderPaymentDetailsProvider:
    """DAO class which providers info for orders specifically in relation to payment details"""

    def __init__(self, postgres=None):
        self.postgres = postgres or res.connectors.load("postgres")

    def get_unpaid_orders_for_brands(self, list_brand_codes: list, start_date):
        query = f"""
            select
                oli.id,
                o.name,
                o.sales_channel,
                o.brand_code,
                o.ecommerce_source ,
                oli.price,
                oli.sku,
                oli.quantity,
                oli.basic_cost_of_one,
                oli.basic_cost_of_one_payment_stamped_at,
                oli.price_payment_stamped_at,
                o.revenue_share_percentage_wholesale ,
                o.revenue_share_percentage_retail,
                o.ordered_at
            from
                sell.orders o
            inner join sell.order_line_items oli  
                        on
                oli.order_id = o.id
            where
                o.ordered_at > %s
                AND o.brand_code in ({','.join([f"'{code}'" for code in list_brand_codes if code])})
                and ( o.payment_status is null
                    or (o.payment_status != 'requested'
                        and o.payment_status != 'succeeded') ) 

"""

        dfOrders = self.postgres.run_query(query, (start_date,))

        dfOrders.sort_values(by=["brand_code", "ordered_at"], ascending=[True, False])

        return dfOrders

    def update_df_with_missing_revenue_share(self, dfOrders, dfBrands):

        # Creating a dictionary from DataFrame dfBrands
        brand_dict = (
            dfBrands.groupby("brand_code")
            .apply(
                lambda x: x[
                    ["payments_revenue_share_wholesale", "payments_revenue_share_ecom"]
                ].to_dict("records")[0]
            )
            .to_dict()
        )

        # For wholesale orders, load the revenue_share_percentage_wholesale from the brand if its missing
        dfOrders["revenue_share_percentage_wholesale"] = dfOrders.apply(
            lambda row: (
                brand_dict[row["brand_code"]]["payments_revenue_share_wholesale"]
                if (
                    row["brand_code"] in brand_dict
                    and pd.isnull(row["revenue_share_percentage_wholesale"])
                    and (row["sales_channel"].lower() == "wholesale")
                )
                else row["revenue_share_percentage_wholesale"]
            ),
            axis=1,
        )

        # For retail orders, load the payments_revenue_share_ecom from the brand if its missing
        dfOrders["revenue_share_percentage_retail"] = dfOrders.apply(
            lambda row: (
                brand_dict[row["brand_code"]]["payments_revenue_share_ecom"]
                if (
                    (
                        row["brand_code"] in brand_dict
                        and pd.isnull(row["revenue_share_percentage_retail"])
                        and (row["sales_channel"].lower() == "ecom")
                    )
                )
                else row["revenue_share_percentage_retail"]
            ),
            axis=1,
        )
        return dfOrders

    def filter_out_orders_with_missing_price_or_costs(self, dfOrders):

        dfMissingCosts = dfOrders[
            dfOrders["basic_cost_of_one"].isna() | (dfOrders["basic_cost_of_one"] == 0)
        ]

        dfMissingRetailPrices = dfOrders[
            (dfOrders["sales_channel"].str.lower() == "ecom")
            & (dfOrders["price"].isna() | (dfOrders["price"] == 0))
        ]

        dfMissingWholesalePrices = dfOrders[
            (dfOrders["sales_channel"].str.lower() == "wholesale")
            & (dfOrders["price"].isna() | (dfOrders["price"] == 0))
        ]

        dfAllMissing = pd.concat(
            [dfMissingRetailPrices, dfMissingWholesalePrices, dfMissingCosts],
            ignore_index=True,
        )

        ordersCantPriceSet = set(dfAllMissing["name"])

        self.alert_orders_cant_price(ordersCantPriceSet)
        dfOrders["missing_cost_price_info"] = dfOrders["name"].isin(ordersCantPriceSet)

        return dfOrders

    def alert_orders_cant_price(self, ordersCantPrice: set):
        pass

    # todo slack ping the shit out of this list
