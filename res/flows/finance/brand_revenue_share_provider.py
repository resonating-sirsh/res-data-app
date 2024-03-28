from tenacity import retry, stop_after_attempt, wait_fixed

import res
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient

GET_ALL_BRANDS_SQL_WITH_PAYMENT_METHODS = """
SELECT 
    b.id as brand_db_pkid, 
    b.brand_code, 
    b.is_brand_whitelist_payment,  
    b."name" as brand_name, 
    contact_email, 
    b.payments_revenue_share_ecom, 
    b.payments_revenue_share_wholesale,
    b.register_brand_email_address, 
    b.start_date as brand_start_date, 
    b.end_date as brand_end_date, 
    s."name" as sub_name, 
    s.subscription_id, 
    s.id as sub_db_pkid,  
    s.collection_method, 
    s.stripe_customer_id, 
    s.payment_method, 
    s.is_direct_payment_default, 
    s.deleted_from
FROM
SELL.BRANDS b
LEFT OUTER JOIN sell.subscriptions s on s.brand_id = b.id
--WHERE s.deleted_from IS NULL or s.deleted_from > CURRENT_TIMESTAMP TODO THIS MUST GO BACK IN
AND b.start_date <= CURRENT_TIMESTAMP and b.end_date >= CURRENT_TIMESTAMP
"""


class BrandRevenueShareProvider:
    @retry(wait=wait_fixed(1), stop=stop_after_attempt(4), reraise=True)
    def get_brands_rev_share_lookup_from_postgres(self, postgres):
        postgres = postgres or res.connectors.load("postgres")
        dfBrands = postgres.run_query(GET_ALL_BRANDS_SQL_WITH_PAYMENT_METHODS)

        dfBrands = dfBrands[
            [
                "brand_code",
                "payments_revenue_share_wholesale",
                "payments_revenue_share_ecom",
            ]
        ]

        return dfBrands

    # this needs a retrier cos graphql is a heap of shit...

    def graph_query_with_retry(self, graphql_client, query, chunk):
        data_styles_invoices = graphql_client.query(
            query,
            {"resonance_code_array": chunk, "after": "0"},
        )

        if not (data_styles_invoices and "data" in data_styles_invoices):
            raise Exception(
                f"data_styles_invoices came back empty from price query {query}"
            )
        else:
            return data_styles_invoices["data"]  #

    def get_cost_price_info_for_skus(self, sku_list, is_prices):
        """
        This function, given a list of skus, will get retail, wholesale and cost info for that list. returned as a dict with sku as the key.
        Bear in mind that not all skus might be found, or cost could be null, or both retail and wholesale might be zero and null(this last one is very common)
        Skus should be in format: 'RB-6018 PIMA7 NATUQX 0ZZXX'
        """
        import pandas as pd

        graphql_client = ResGraphQLClient()

        # break down the skus in to a list of dicts with sku as key and code and size as values in dict #
        sku_broken_down_list = self._get_decontructed_sku_list(sku_list)

        # now get all the styles, we need this for the query
        all_styles_array = [
            style_sku_info["code"] for style_sku_info in sku_broken_down_list
        ]

        code_to_skus_lookup = {}
        for item in sku_broken_down_list:
            code = item["code"]
            sku = item["sku"]
            code_to_skus_lookup.setdefault(code, []).append(sku)

        res.utils.logger.info(
            f"Requesting ({len(all_styles_array)}) unique style codes"
        )
        # remove dupes
        all_styles_array = list(set(all_styles_array))
        pricing_lookup_sku = {}

        # because createOneAPI is slow, you need to chunk the style codes into small chunks or you'll get timeouts
        chunk_size = 50
        for i in range(0, len(all_styles_array), chunk_size):
            chunk = all_styles_array[i : i + chunk_size]

            res.utils.logger.info(f"Requesting ({i} to  {i + chunk_size}) from graphQL")
            query_to_use = (
                self.GET_STYLES_PRICES if is_prices else self.GET_STYLES_COSTS
            )
            data_styles_invoices = self.graph_query_with_retry(
                graphql_client, query_to_use, chunk
            )

            # now we take the mess of a dict that the graphQL query returns and create a succint dict to return to only show our 3 values (cost, retail, wholesale)

            res.utils.logger.info(
                f"retuned count: {len(data_styles_invoices['styles']['styles'])}"
            )
            for style in data_styles_invoices["styles"]["styles"]:
                if is_prices:
                    code = style["code"]

                    for sku in code_to_skus_lookup[code]:

                        wholesalePrice = style.get("allProducts")[0].get(
                            "wholesalePrice"
                        )
                        retailPrice = style.get("allProducts")[0].get("price")

                        pricing_lookup_sku[sku] = {
                            "wholesaleprice": wholesalePrice,
                            "retailprice": retailPrice,
                        }
                else:
                    for onePrice in style["onePrices"]:
                        sizeCode = onePrice.get("size").get("code")
                        sku = style["code"] + " " + sizeCode

                        cost = round(onePrice.get("cost"), 2)

                        pricing_lookup_sku[sku] = {"cost": cost}

        # the next couple of steps are not essential, just capturing some stats to report what could / could not be found
        pricing_lookup_sku = {
            k: v for k, v in pricing_lookup_sku.items() if k in sku_list
        }
        skus_returned_graphql = set([sku for sku in pricing_lookup_sku.keys()])

        skus_requested = set(sku_list)
        skus_not_returned = skus_requested.difference(skus_returned_graphql)
        res.utils.logger.info(
            f"Skus missing from results: ({len(skus_not_returned)}) {skus_not_returned}"
        )

        # delete the extra stuff the graph has returned to us

        df = pd.DataFrame(
            [
                {
                    "sku": k,
                    "retailprice": v.get("retailprice"),
                    "wholesaleprice": v.get("wholesaleprice"),
                    "cost": v.get("cost"),
                }
                for k, v in pricing_lookup_sku.items()
            ]
        )
        return df


if __name__ == "__main__":

    pricecalc = SkuPriceCostCalculator()
    skus = pricecalc._generate_list_of_skus()

    d = pricecalc.get_cost_price_info_for_skus(skus)
    res.utils.logger.info(res.utils.safe_json_dumps(d))
