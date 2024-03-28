import json
import traceback

import pandas as pd

import res
from res.flows.finance.brand_revenue_share_provider import BrandRevenueShareProvider
from res.flows.finance.sku_price_cost_calculator import SkuPriceCostCalculator
from res.utils import logger, ping_slack
from res.utils.kafka_utils import message_produce

slack_id_to_notify = " <@U04HYBREM28> "

# nudge
payments_api_slack_channel = "payments_api_notification"

# a pure hack as users have been mislablleing C1 order types. delete this in a couple of weeks
orders_to_temp_ignore = """
AS-2148079
AS-2148080
AS-2148083
AS-2148084
AS-2148085
AS-2148086
AS-2148087
AS-2148088
AS-2148089
AS-2148090
AS-2148091
AS-2148092
AS-2148342
LA-2148008
TK-2148277
TK-2148403"""


class OrderCostPriceResolver:
    """This class will traverse recent order line items in postgres sell.order_line_items (going back X days)
    It will look for rows where price is 0 and should not be, and look for cost (BCOO) not being set, and pull this data from the graphQL, and then update the offending rows
    sell.order_line_items that done have prices and costs, cant be billed for, so we need to fix them up, so they can be billed for.  In general we should not be makign them either, but are, as of Mar 2024.
    This can be ran as part of payment and it should also be ran regularly during the day to check for order line items missing costs.
    what often happen is we sell styles that do not yet have price (retail/wholesale) associated with them yet
    """

    BRANDS_TO_IGNORE = " 'TT','RS' "  # these 2 are resonance test brands

    base_select_and_where = """ 
            select
                oli.id,
                o.name,
                o.sales_channel,
                o.brand_code,
                o.ecommerce_source ,
                o.revenue_share_percentage_wholesale,
                o.revenue_share_percentage_retail,
                oli.price,
                oli.sku,
                oli.quantity,
                oli.basic_cost_of_one,
                oli.basic_cost_of_one_payment_stamped_at,
                oli.price_payment_stamped_at,
                oli.revenue_share_amount,
                oli.line_total_charge_amount,
                oli.line_total_charge_amount_stamped_at
            from
                sell.orders o
            inner join sell.order_line_items oli  
                        on
                oli.order_id = o.id
            where
                o.ordered_at > CURRENT_DATE - interval '%s days'    
            """
    order_nums = "','".join(orders_to_temp_ignore.strip().split("\n"))

    generic_filter_ignore_orders_brands = f"""
    and ( o.brand_code not in ({BRANDS_TO_IGNORE}))
            and (o.name not in ('{order_nums}'))
    order by
            o.ordered_at desc
    """

    def _get_recent_orders_missing_costs(self, days_back: int, postgres):
        """
        Get Order and Order line items going back X days
        where either price = 0 and its a type that we would expect a retail or wholesale price ot be set for (not 'GIFTING', 'Development', 'PHOTO', 'First ONE' ) and we have not yet tried to set it
        OR bcoo is zero and we have not yet tried to set"""

        postgres = postgres or res.connectors.load("postgres")

        query = f"""
            
        {self.base_select_and_where}
       
            and (
            --cost info not yet set
                    (oli.basic_cost_of_one = 0
                or oli.basic_cost_of_one is null)
            and oli.basic_cost_of_one_payment_stamped_at is null)
                    
            {self.generic_filter_ignore_orders_brands}
           
        """
        dfOrderLines = postgres.run_query(query, (days_back,))
        return dfOrderLines

    def _get_recent_orders_missing_line_total_amounts(self, days_back: int, postgres):
        """
        Get Order and Order line items going back X days
        where either price = 0 and its a type that we would expect a retail or wholesale price ot be set for (not 'GIFTING', 'Development', 'PHOTO', 'First ONE' ) and we have not yet tried to set it
        OR bcoo is zero and we have not yet tried to set"""

        postgres = postgres or res.connectors.load("postgres")

        query = f"""
        {self.base_select_and_where}
            and ( 
                (oli.line_total_charge_amount = 0
                or oli.line_total_charge_amount is null) 
                and oli.basic_cost_of_one is not null)

            {self.generic_filter_ignore_orders_brands}
        """
        dfOrderLines = postgres.run_query(query, (days_back,))
        return dfOrderLines

    def _get_recent_orders_missing_prices(self, days_back: int, postgres):
        """
        Get Order and Order line items going back X days
        where either price = 0 and its a type that we would expect a retail or wholesale price ot be set for (not 'GIFTING', 'Development', 'PHOTO', 'First ONE' ) and we have not yet tried to set it
        OR bcoo is zero and we have not yet tried to set"""
        postgres = postgres or res.connectors.load("postgres")

        query = f"""
            
            {self.base_select_and_where}
            
                and (
                --price is zero or not yet and we have not yet tried to set it and its in a sales channel that we charge a retail or wholesale price for
                        (oli.price = 0
                    or oli.price is null)
                and (oli.price_payment_stamped_at is null ))
                    and lower(o.sales_channel) in ('wholesale', 'ecom') 
                        
                {self.generic_filter_ignore_orders_brands}
            
        """
        dfOrderLines = postgres.run_query(query, (days_back,))
        return dfOrderLines

    def _get_recent_orders_missing_revenue_share_amount(self, days_back: int, postgres):
        """
        Get Order and Order line items going back X days
        where either price = 0 and its a type that we would expect a retail or wholesale price ot be set for (not 'GIFTING', 'Development', 'PHOTO', 'First ONE' ) and we have not yet tried to set it
        OR bcoo is zero and we have not yet tried to set"""
        postgres = postgres or res.connectors.load("postgres")
        query = f"""
            {self.base_select_and_where}
            
                and (
                    (oli.revenue_share_amount is null
                        and (oli.revenue_share_amount_stamped_at is null ))
                   
                )
            
                and lower(o.sales_channel) in ('wholesale', 'ecom') 
                        
                 {self.generic_filter_ignore_orders_brands}
            
        """
        dfOrderLines = postgres.run_query(query, (days_back,))
        return dfOrderLines

    def _calc_line_order_total_amount(self, dfOrders, timestamp_now):
        def calculate_line_order_total_charge(row):
            if row["sales_channel"].lower() in ["ecom", "wholesale"]:
                if not row["revenue_share_amount"] is None:
                    dfOrders["line_total_charge_amount_stamped_at"] = timestamp_now
                    return row["revenue_share_amount"] + row["basic_cost_of_one"]
                else:
                    return None
            elif row["sales_channel"].lower() == "first one":
                dfOrders["line_total_charge_amount_stamped_at"] = timestamp_now
                return 0
            else:
                dfOrders["line_total_charge_amount_stamped_at"] = timestamp_now
                return row["basic_cost_of_one"]

        dfOrders["line_total_charge_amount"] = dfOrders.apply(
            calculate_line_order_total_charge, axis=1
        )

        return dfOrders

    def update_orders_with_missing_revenue_share(
        self, dfOrders, dfbrand_rev_share_lookup
    ):

        # if the revenuue share percetain for retail/wholesale not set on the order, read it from brand dicts, and set that on the orders object, for later save to DB

        dfmerged = dfOrders.merge(dfbrand_rev_share_lookup, how="left", on="brand_code")

        dfmergedEcom = dfmerged[dfmerged["sales_channel"].str.lower() == "ecom"].copy()
        dfmergedWholesale = dfmerged[
            dfmerged["sales_channel"].str.lower() == "wholesale"
        ].copy()

        dfmergedWholesale.loc[
            :, "revenue_share_percentage_wholesale"
        ] = dfmergedWholesale["payments_revenue_share_wholesale"]

        dfmergedEcom.loc[:, "revenue_share_percentage_retail"] = dfmergedEcom[
            "payments_revenue_share_ecom"
        ]

        dfHasNullRetailRevShare = dfmergedEcom[
            dfmergedEcom["revenue_share_percentage_retail"].isna()
        ]

        dfHasZeroRetailRevShare = dfmergedEcom[
            dfmergedEcom["revenue_share_percentage_retail"] == 0
        ]

        dfHasNullWholesaleRevShare = dfmergedWholesale[
            dfmergedWholesale["revenue_share_percentage_wholesale"].isna()
        ]

        dfHasZeroWholesaleRevShare = dfmergedWholesale[
            dfmergedWholesale["revenue_share_percentage_wholesale"] == 0
        ]

        zerorateWholesaleBrands = set(dfHasZeroWholesaleRevShare["brand_code"])
        zerorateRetailBrands = set(dfHasZeroRetailRevShare["brand_code"])
        wholesaleOrdersBrandsHaveNoRetailRevShare = (
            dfHasNullWholesaleRevShare.groupby("brand_code")["name"]
            .apply(list)
            .reset_index()
            .apply(tuple, axis=1)
            .tolist()
        )
        ecomOrdersBrandsHaveNoRetailRevShare = (
            dfHasNullRetailRevShare.groupby("brand_code")["name"]
            .apply(list)
            .reset_index()
            .apply(tuple, axis=1)
            .tolist()
        )

        if (
            ecomOrdersBrandsHaveNoRetailRevShare
            or wholesaleOrdersBrandsHaveNoRetailRevShare
        ):
            ecom_str = "\n, ".join(
                [
                    f"{brand_code}: {orders}"
                    for brand_code, orders in ecomOrdersBrandsHaveNoRetailRevShare
                ]
            )
            wholesale_str = "\n, ".join(
                [
                    f"{brand_code}: {orders}"
                    for brand_code, orders in wholesaleOrdersBrandsHaveNoRetailRevShare
                ]
            )
            output_str = f"Ecom: [{ecom_str}]\nWholesale: [{wholesale_str}]"

            ping_slack(
                f"[BRANDS WARNING] The following brands and orders have no retail share agreement percentage set.  This needs to resovled or these orders cannot be billed for. \n{output_str}",
                "skus_missing_prices_or_costs",
            )
        if zerorateRetailBrands:
            ping_slack(
                f"[BRANDS WARNING] The following brands have zero percent RETAIL rev share agreement: {zerorateRetailBrands}",
                "skus_missing_prices_or_costs",
            )
        if zerorateWholesaleBrands:
            ping_slack(
                f"[BRANDS WARNING] The following brands have zero percent WHOLESALE rev share agreement: {zerorateWholesaleBrands}",
                "skus_missing_prices_or_costs",
            )

        dfOrders = pd.concat([dfmergedWholesale, dfmergedEcom], ignore_index=True)

        return dfOrders

    def apply_rev_share_amount(self, dfOrderLinesMissingRevShare, timestamp_now):
        def row_based_apply_rev_share(row):
            if row["sales_channel"] == "ECOM" and not pd.isna(
                row["revenue_share_percentage_retail"]
            ):
                dfOrderLinesMissingRevShare[
                    "revenue_share_amount_stamped_at"
                ] = timestamp_now
                return row["price"] * row["revenue_share_percentage_retail"]

            elif row["sales_channel"].lower() == "wholesale" and not pd.isna(
                row["revenue_share_percentage_wholesale"]
            ):
                dfOrderLinesMissingRevShare[
                    "revenue_share_amount_stamped_at"
                ] = timestamp_now
                return row["price"] * row["revenue_share_percentage_wholesale"]

            else:
                return None

        dfOrderLinesMissingRevShare[
            "revenue_share_amount"
        ] = dfOrderLinesMissingRevShare.apply(
            row_based_apply_rev_share,
            axis=1,
        )

        return dfOrderLinesMissingRevShare

    def determine_orderLineItemsPriceFixes(
        self, merged_df, timestamp_now, days_back, publish_missing_sku_nag
    ):
        # get rows where price not set and not yet having a stamp saying we set the price
        no_price_df = merged_df[(merged_df["price"].isna() | merged_df["price"] == 0)]
        no_price_df = no_price_df[no_price_df["price_payment_stamped_at"].isna()]

        # and not in the set of sales channells where we dont charge a retail or wholesale price
        no_price_df = no_price_df[
            no_price_df["sales_channel"].str.lower().isin(["ecom", "wholesale"])
        ]

        # exlude those rows that now dont have a price set (style failed to get us a price from graphQL.) we cant fix this these rows so remove from our update for DB data frame
        # if ecom and retail Price is null- cant update to remove it from DF to be saved to DB
        style_prices_missing_no_price_df = no_price_df[
            ~(
                (
                    no_price_df["retailprice"].isna()
                    | (no_price_df["retailprice"].isnull())
                )
                & (no_price_df["sales_channel"].str.lower() == "ecom")
            )
        ]  #
        # if wholesale and wholesale Price is null- cant update to remove it from DF to be saved to DB #
        style_prices_missing_no_price_df = style_prices_missing_no_price_df[
            ~(
                (
                    (style_prices_missing_no_price_df["wholesaleprice"].isna())
                    | (style_prices_missing_no_price_df["wholesaleprice"].isnull())
                )
                & (
                    style_prices_missing_no_price_df["sales_channel"].str.lower()
                    == "wholesale"
                )
            )
        ]

        # by now we have a df we can update all rows. set wholesaleprice or retail to price, depending on sales channel type
        # had to rewrite this away from lambda as black keeps fucking it up ##
        def determine_price(row):
            if row["sales_channel"] == "Wholesale":
                return row["wholesaleprice"]
            elif row["sales_channel"] == "ECOM":
                return row["retailprice"]
            else:
                return row["price"]

        style_prices_missing_no_price_df[
            "price"
        ] = style_prices_missing_no_price_df.apply(determine_price, axis=1)

        # this is a stamping action for audit and to say we saved prices on this row etc.  used in future queries to eliminate these rows from future attempts
        style_prices_missing_no_price_df["price_payment_stamped_at"] = timestamp_now

        # figure out the orders and skus we could not get prices for. report these for resolution.
        cant_find_prices_df = no_price_df[
            ~no_price_df.index.isin(style_prices_missing_no_price_df.index)
        ]
        cant_find_prices_df = cant_find_prices_df[
            ["name", "sku", "ecommerce_source", "sales_channel"]
        ]

        missing_prices_str = "\n".join(
            [",  ".join(map(str, row)) for row in cant_find_prices_df.values]
        )

        output = f"""
        Found prices for: {len(style_prices_missing_no_price_df)} order line items. There were {len(no_price_df)} order line items missing prices. We have checked backwards for {days_back} days.
        Therefore, we are missing PRICES for:\n{missing_prices_str}
        """
        res.utils.logger.info(output)
        ping_slack(
            f"[OrderCostPriceResolver.check_and_fix_orders_for_recent_days] {output}",
            "payments_api_verbose",
        )
        if publish_missing_sku_nag:
            ping_slack(
                f"SKUs missing PRICES update: \n {output}",
                "skus_missing_prices_or_costs",
            )

        # todo, at some point we will need to alert/kafka message on the orders we DID update a price for. Some day, these will be held for not having prices.  Either here, or after they are paid for, is the  time they need to be unblocked
        return style_prices_missing_no_price_df

    def check_and_fix_orders_for_recent_days(
        self,
        days_back_cost=11,
        days_back_price_check=20,
        postgres=None,
        publish_missing_sku_nag=True,
    ):
        """
        Main orchestrator. See class header doc for details of what this does
        """
        try:
            postgres = postgres or res.connectors.load("postgres")
            timestamp_now = res.utils.dates.utc_now()

            logger.info(
                f"Running check_and_fix_orders_for_recent_days with timestamp {timestamp_now}.  Rows update in sell.order_line_items will have this key for oli.basic_cost_of_one_payment_stamped_at, oli.price_payment_stamped_at "
            )
            self.process_missing_costs_on_orders(
                days_back_cost, postgres, timestamp_now
            )

            #################################
            # Prices from here down
            #########################

            self.process_missing_prices_on_orders(
                days_back_price_check, postgres, publish_missing_sku_nag, timestamp_now
            )

            #################################
            # Rev share amounts
            #########################
            self.process_missing_revenue_share_amount_on_orders(
                days_back_price_check, postgres, timestamp_now
            )

            #################################
            # Line total charge amounts
            #########################
            self.sum_up_line_totals_on_orders(days_back_cost, postgres, timestamp_now)

        except:

            msg = f"[OrderCostPriceResolver.check_and_fix_orders_for_recent_days] ERROR. \n\n{traceback.format_exc()} {slack_id_to_notify}"

            logger.warn(msg)
            ping_slack(
                msg,
                payments_api_slack_channel,
            )

    def sum_up_line_totals_on_orders(self, days_back_cost, postgres, timestamp_now):
        dfOrderLinesMissingTotalChargeAmount = (
            self._get_recent_orders_missing_line_total_amounts(days_back_cost, postgres)
        )
        logger.info(
            f"Retrieved {len(dfOrderLinesMissingTotalChargeAmount)} sell.order_line_items rows that have missing_line_total_amounts issues going back {days_back_cost} days"
        )

        dfOrderLinesMissingTotalChargeAmount = self._calc_line_order_total_amount(
            dfOrderLinesMissingTotalChargeAmount, timestamp_now
        )

        line_total_charge_amount_dicts = dfOrderLinesMissingTotalChargeAmount[
            [
                "id",
                "line_total_charge_amount",
                "line_total_charge_amount_stamped_at",
            ]
        ].to_dict("records")

        logger.info(
            f"Now save these {len(line_total_charge_amount_dicts)} rows for update sell.order_line_items to DB with the new line_total_charge_amount_dicts amounts set. timestamp on rows: {timestamp_now}"
        )
        for line_amt_dict in line_total_charge_amount_dicts:
            if len(line_amt_dict["id"]) > 20:
                postgres.update_records(
                    "sell.order_line_items",
                    line_amt_dict,
                    {"id": line_amt_dict["id"]},
                    keep_conn_open=True,
                )

        logger.info(
            f"line_total_charge_amount_dicts updates saved to postgres... Action completed"
        )

    def process_missing_revenue_share_amount_on_orders(
        self, days_back_price_check, postgres, timestamp_now
    ):
        dfOrderLinesMissingRevShare = (
            self._get_recent_orders_missing_revenue_share_amount(
                days_back_price_check, postgres
            )
        )
        logger.info(
            f"Retrieved {len(dfOrderLinesMissingRevShare)} sell.order_line_items rows that have revenue share agreement issues going back {days_back_price_check} days"
        )
        brand_provider = BrandRevenueShareProvider()
        dfBrandRevShareLookup = (
            brand_provider.get_brands_rev_share_lookup_from_postgres(postgres)
        )

        if not dfOrderLinesMissingRevShare.empty:
            dfOrderLinesMissingRevShare = self.update_orders_with_missing_revenue_share(
                dfOrderLinesMissingRevShare, dfBrandRevShareLookup
            )

            dfOrderLinesMissingRevShare = self.apply_rev_share_amount(
                dfOrderLinesMissingRevShare, timestamp_now
            )

            list_price_rev_share_amount_dicts = dfOrderLinesMissingRevShare[
                [
                    "id",
                    "revenue_share_amount",
                    "revenue_share_amount_stamped_at",
                ]
            ].to_dict("records")

            logger.info(
                f"Now save these {len(list_price_rev_share_amount_dicts)} rows for update sell.order_line_items to DB with the new rev share amounts set. timestamp on rows: {timestamp_now}"
            )
            for rev_share_amt_dict in list_price_rev_share_amount_dicts:
                if len(rev_share_amt_dict["id"]) > 20:
                    postgres.update_records(
                        "sell.order_line_items",
                        rev_share_amt_dict,
                        {"id": rev_share_amt_dict["id"]},
                        keep_conn_open=True,
                    )

            logger.info(
                f"rev share amounts updates saved to postgres... Action completed"
            )

    def process_missing_prices_on_orders(
        self, days_back_price_check, postgres, publish_missing_sku_nag, timestamp_now
    ):
        dfOrderLinesMissingPrices = self._get_recent_orders_missing_prices(
            days_back_price_check, postgres
        )
        logger.info(
            f"Retrieved {len(dfOrderLinesMissingPrices)} sell.order_line_items rows that have pricing issues going back {days_back_price_check} days"
        )
        skuPriceCalc = SkuPriceCostCalculator()
        sku_list_prices = list(set(dfOrderLinesMissingPrices["sku"]))
        # Go off to the graph to get prices. this can be very slow.
        dfSkuPrices = skuPriceCalc.get_cost_price_info_for_skus(
            sku_list_prices, is_prices=True
        )

        if not dfOrderLinesMissingPrices.empty and not dfSkuPrices.empty:
            merged_df_prices = dfOrderLinesMissingPrices.merge(
                dfSkuPrices, on="sku", how="left"
            )

            # get prices and merge them into data frame for those rows that are missing price info but should have it
            dfPriceUpdates = self.determine_orderLineItemsPriceFixes(
                merged_df_prices,
                timestamp_now,
                days_back_price_check,
                publish_missing_sku_nag,
            )

            list_price_update_dict = dfPriceUpdates[
                ["id", "price", "price_payment_stamped_at"]
            ].to_dict("records")

            logger.info(
                f"Now save these {len(list_price_update_dict)} rows for update sell.order_line_items to DB with the new prices set. timestamp on rows: {timestamp_now}"
            )
            for price_dict in list_price_update_dict:
                if len(price_dict["id"]) > 20:
                    postgres.update_records(
                        "sell.order_line_items",
                        price_dict,
                        {"id": price_dict["id"]},
                        keep_conn_open=True,
                    )

            logger.info(f"price updates saved to postgres... ")
        else:
            logger.info(f"No order_line_items in postgres are missing price info")

    def process_missing_costs_on_orders(self, days_back_cost, postgres, timestamp_now):
        dfOrderLinesMissingCosts = self._get_recent_orders_missing_costs(
            days_back_cost, postgres
        )
        logger.info(
            f"Retrieved {len(dfOrderLinesMissingCosts)} sell.order_line_items rows that have BCOO issues going back {days_back_cost} days"
        )
        sku_list = list(set(dfOrderLinesMissingCosts["sku"]))
        skuPriceCalc = SkuPriceCostCalculator()
        # Go off to the graph to get prices. this can be very slow.
        dfSkuCosts = skuPriceCalc.get_cost_price_info_for_skus(
            sku_list, is_prices=False
        )
        if not dfOrderLinesMissingCosts.empty and not dfSkuCosts.empty:
            # merge the retreieved prices into a merged Df, joining by SKU
            merged_df_costs = dfOrderLinesMissingCosts.merge(
                dfSkuCosts, on="sku", how="left"
            )

            # find rows that have no cost data set and we have not yet treid to set the cost (the stamp basic_cost_of_one_payment_stamped_at)
            no_cost_df = merged_df_costs[
                (pd.isnull(merged_df_costs["basic_cost_of_one"]))
                & (pd.isnull(merged_df_costs["basic_cost_of_one_payment_stamped_at"]))
            ]

            no_cost_df["basic_cost_of_one"] = no_cost_df["cost"]
            no_cost_df["basic_cost_of_one_payment_stamped_at"] = timestamp_now

            noCostForStyleDf = no_cost_df[
                no_cost_df["basic_cost_of_one"].isna()
                | no_cost_df["basic_cost_of_one"].isnull()
            ]
            if not noCostForStyleDf.empty:
                missingOutput = f"""
                    Orders missing cost info: {set(noCostForStyleDf['name'])}
                    Skus missing cost info on those orders: {set(noCostForStyleDf['sku'])}
                    """
                ping_slack(missingOutput, "payments_api_verbose")
                ping_slack(
                    f"SKUs missing COSTS update: \n {missingOutput}",
                    "skus_missing_prices_or_costs",
                )
                logger.info(missingOutput)

            list_costs_update_dict = no_cost_df[
                ["id", "basic_cost_of_one", "basic_cost_of_one_payment_stamped_at"]
            ].to_dict("records")

            logger.info(
                f"Now save these {len(list_costs_update_dict)} update sell.order_line_items to DB with the new bcoo set. timestamp on rows: {timestamp_now}"
            )
            for cost_dict in list_costs_update_dict:
                if len(cost_dict["id"]) > 20:
                    postgres.update_records(
                        "sell.order_line_items",
                        cost_dict,
                        {"id": cost_dict["id"]},
                        keep_conn_open=True,
                    )

            logger.info(f"BCOO updates saved to postgres")
        else:
            logger.info(f"No order_line_items in postgres are missing BCOO info")
            # todo reraise


if (__name__) == "__main__":
    c = OrderCostPriceResolver()
    postgres = res.connectors.load("postgres")

    c.check_and_fix_orders_for_recent_days(postgres=postgres)
