import math
import os
from datetime import datetime

from res.utils import logger
from res.connectors.graphql.hasura import Client
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import json
from res.flows.make.payment.queries import GET_CURRENT_INVOICE, GET_STYLES
import res.connectors
from res.utils import secrets_client, ping_slack, logger, safe_json_serialiser


class NotMatchSizeInStyle(Exception):
    pass


# nudge

slack_id_to_notify = " <@U04HYBREM28> "  # damien liddane


class PaymentInvoiceCalculator:
    """This process calculates the invoice amounts for orders to be made."""

    def __init__(self):
        self.hasura_client = Client()
        self.graphql_client = ResGraphQLClient()
        self.shipping_amount = 20

    def __search_lineitem_infos_and_add_line_item_price(
        self, order_line_items_info_array, sku
    ):
        dolar_price_total = 0
        for line_item_info in order_line_items_info_array:
            if line_item_info["sku"] == sku:
                dolar_price_total = dolar_price_total + float(
                    line_item_info["lineItemPrice"]
                )

        return dolar_price_total

    ##TODO: addd wholwsale price * rev_shared?
    def __add_wholesale_prices(
        self, all_products_array, sku, revenue_share_wholesale_rate
    ):
        dollar_price_total = 0
        # data["order"]["brand"].get("revenueShareWholesale")
        fee = 0
        for product in all_products_array:
            logger.info(product)
            dollar_price_total = dollar_price_total + product["wholesalePrice"]
            fee = fee + (revenue_share_wholesale_rate * dollar_price_total)

        return dollar_price_total

    def __iterate_skus_and_add_totals(
        self,
        data_styles_invoices,
        sales_channel,
        line_item_tuple,
        size_id,
        data,
        dolar_price_total,
        dolar_cost_total,
        revenue_share_rate,
        order_number,
        order_date,
        revenue_share_wholesale_rate,
    ):
        payment_details = []
        logger.info(f"Size ID: {size_id}")
        for style in data_styles_invoices["styles"]["styles"]:
            if style["code"] == line_item_tuple["code"]:
                for prices in style["onePrices"]:
                    if prices["size"]["code"] == size_id:
                        make_cost = 0
                        if prices["cost"]:
                            make_cost = prices["cost"]
                        dolar_cost_total = dolar_cost_total + make_cost
                        item_price = 0
                        is_create_one_order = False
                        if (
                            sales_channel == "ECOM"
                            and data["order"]["orderChannel"] == "Shopify"
                        ):
                            item_price = (
                                self.__search_lineitem_infos_and_add_line_item_price(
                                    data["order"]["lineItemsInfo"],
                                    line_item_tuple["sku"],
                                )
                            )
                            dolar_price_total = dolar_price_total + item_price
                        elif sales_channel == "Wholesale":
                            item_price = self.__add_wholesale_prices(
                                style["allProducts"],
                                line_item_tuple["sku"],
                                revenue_share_wholesale_rate,
                            )
                            dolar_price_total = dolar_price_total + item_price
                        else:
                            is_create_one_order = True
                            print(f"Is a Create on Order sku")

                        price_with_revenue = 0
                        price_amount = 0
                        if not is_create_one_order:
                            price_with_revenue = item_price * revenue_share_rate
                            price_amount = item_price

                        payment_details.append(
                            {
                                "make_cost": round(make_cost, 2),
                                "price": round(price_amount, 2),
                                "shipping": self.shipping_amount,
                                "order_number": order_number,
                                "order_date": order_date,
                                "order_type": sales_channel,
                                "revenue_share": round(price_with_revenue, 2),
                                "total_amount": round(
                                    price_with_revenue + make_cost, 2
                                ),
                                "sku": line_item_tuple["sku"],
                            }
                        )
        logger.info(payment_details)

        if not payment_details:
            msg = f"[ACTIONS CALCULATOR] There is no size in style to match/ Sku not valid. {order_number} {line_item_tuple} {slack_id_to_notify}"
            ping_slack(msg, "payments_api_notification")
            raise NotMatchSizeInStyle(msg)

        return {
            "retail_price_total": round(dolar_price_total, 2),
            "make_cost_total": round(dolar_cost_total, 2),
            "details": payment_details,
        }

    def process(self, event):
        return self.get_prices_for_skus_on_order(event)

    def get_prices_for_skus_on_order(self, event):
        # logger.info(event)
        order_invoice_data = None
        logger.info("running GET_CURRENT_INVOICE")
        order_invoice_data = self.graphql_client.query(
            GET_CURRENT_INVOICE,
            {"order_id": event["order_id"]},
        )

        if not order_invoice_data or "data" not in order_invoice_data:
            return {
                "error": "No data on main Invoice query.",
                "valid": False,
                "reason": "Not balance available",
            }

        order_invoice_data = order_invoice_data.get("data")
        logger.info(
            f"invoice data for {event['order_id']} {json.dumps(order_invoice_data, default=res.utils.safe_json_serialiser, indent=4)}"
        )
        order_number = (
            order_invoice_data["order"]["brand"]["code"]
            + "-"
            + order_invoice_data["order"]["number"]
        )
        brand_name = order_invoice_data["order"]["brand"]["name"]

        revenue_share_string = order_invoice_data["order"]["brand"].get(
            "revenueShareEcommerce"
        )
        revenue_share_wholesale_string = order_invoice_data["order"]["brand"].get(
            "revenueShareWholesale"
        )
        sales_channel = order_invoice_data["order"]["salesChannel"]

        order_date = order_invoice_data["order"]["createdAt"]

        sku_array = [
            line_item_info["sku"]
            for line_item_info in order_invoice_data["order"]["lineItemsInfo"]
        ]

        sku_prices_lookup_pg = self.get_sku_prices_from_postgres(order_number)
        totals_result = self.get_totals_result_from_cached(
            sku_array, sku_prices_lookup_pg
        )
        make_cost_total = float(totals_result.get("make_cost_total", 0))
        retail_price_total = float(totals_result.get("retail_price_total", 0))
        dict_array_details = totals_result["data"]

        revenue_share_rate = 0
        revenue_share_wholesale_rate = 0
        if revenue_share_string is not None:
            revenue_share_rate = float(revenue_share_string)
        if revenue_share_wholesale_string is not None:
            revenue_share_wholesale_rate = float(revenue_share_wholesale_string)

        sku_info_list = self.get_decontructed_sku_list(
            sku_array
        )  # gets list of sku, style, size etc

        logger.info("===============================")
        logger.info("SKUs on the order:")
        logger.info(sku_info_list)

        if (
            order_invoice_data["order"]
            and order_invoice_data["order"]["wasPaymentSuccessful"]
        ):
            return {"error": True, "reason": "Order is paid already."}

        if totals_result["missing_sku"]:
            after_style_id_string = "0"
            data_styles_invoices = None

            all_styles_array = [
                style_sku_info["code"] for style_sku_info in sku_info_list
            ]

            logger.info(f"all_styles_array:")
            logger.info(f"=================================:")
            logger.info(
                f"{json.dumps(all_styles_array, default=res.utils.safe_json_serialiser, indent=4)}"
            )

            total_count = 1
            current_style_number = 0

            while total_count > current_style_number:
                current_style_number += 100
                logger.info(f"after_style_cursor_string: {after_style_id_string}")

                data_styles_invoices = self.graphql_client.query(
                    GET_STYLES,
                    {
                        "resonance_code_array": all_styles_array,
                        "after": after_style_id_string,
                    },
                )

                if data_styles_invoices and "data" in data_styles_invoices:
                    data_styles_invoices = data_styles_invoices.get("data")

                logger.info("\n\n============================")
                logger.info("\n\ndata_styles_invoices: ")
                logger.info(
                    json.dumps(
                        data_styles_invoices,
                        default=res.utils.safe_json_serialiser,
                        indent=4,
                    )
                )
                logger.info("")
                logger.info("")
                logger.info(
                    f'Count from query: {data_styles_invoices["styles"]["count"]}'
                )

                total_count = 0
                if data_styles_invoices.get("styles", {}).get("count", 0) > 0:
                    total_count = data_styles_invoices["styles"]["count"]
                    after_style_id_string = (
                        (data_styles_invoices["styles"]["cursor"])
                        if data_styles_invoices.get("styles").get("cursor")
                        else ("0")
                    )
                    logger.info(
                        f"total_count_info: {total_count} after_style_cursor_string: {after_style_id_string}"
                    )

                for line_item_tuple in sku_info_list:
                    size_id = None
                    if line_item_tuple["size"]:
                        size_id = line_item_tuple["size"]

                    totals_result = self.__iterate_skus_and_add_totals(
                        data_styles_invoices,
                        sales_channel,
                        line_item_tuple,
                        size_id,
                        order_invoice_data,
                        retail_price_total,
                        make_cost_total,
                        revenue_share_rate,
                        order_number,
                        order_date,
                        revenue_share_wholesale_rate,
                    )
                    retail_price_total = totals_result["retail_price_total"]
                    make_cost_total = totals_result["make_cost_total"]
                    dict_array_details.extend(totals_result["details"])

                renaming_map = {
                    "price": "retail_price",
                    "revenue_share": "revenue_share_amount",
                }

                save_to_pg_dict_array_details = []
                for d in dict_array_details:
                    d["revenue_share_percentage"] = revenue_share_rate
                    d = {renaming_map.get(k, k): v for k, v in d.items()}
                    d["id"] = res.utils.uuid_str_from_dict(d)
                    save_to_pg_dict_array_details.append(d)

                # de-duplicate the list based on id
                save_to_pg_dict_array_details = list(
                    {d["id"]: d for d in save_to_pg_dict_array_details}.values()
                )

            self.save_sku_prices_to_postgres(save_to_pg_dict_array_details)
        else:
            logger.info(
                "cached prices for all skus in order were all found in postgres"
            )

        total_resonance_cost = (
            make_cost_total
            + (revenue_share_rate * retail_price_total)
            + self.shipping_amount
        )
        total_resonance_cents = math.ceil(total_resonance_cost * 100)
        logger.info(f"total_resonance_cents {total_resonance_cents}")
        return {
            "total_resonance_cents": total_resonance_cents,
            "dict_array_details": dict_array_details,
            "brand_name": brand_name,
            "order_number": order_number,
            "data": order_invoice_data,  # think we no longer need this data
        }

    def save_sku_prices_to_postgres(self, dict_array_details):
        if dict_array_details:
            postgres = res.connectors.load("postgres")

            postgres.run_update(
                f"DELETE FROM sell.order_line_item_pricing WHERE order_number = '{dict_array_details[0]['order_number']}'"
            )
            postgres.insert_records("sell.order_line_item_pricing", dict_array_details)
            logger.info(f"Saved sku pricing for {len(dict_array_details)} skus)")

    def get_totals_result_from_cached(self, sku_array, sku_price_lookup):
        if sku_price_lookup.items():
            missing_sku = False
        else:
            missing_sku = True

        cost_total = 0
        retail_price_total = 0
        full_output_list = []
        for sku in sku_array:
            if sku_price_lookup.get(sku):
                cost_total += sku_price_lookup[sku]["make_cost"]
                retail_price_total += sku_price_lookup[sku]["price"]
                full_output_list.append(sku_price_lookup.get(sku))
            else:
                missing_sku = True

        totals_result = {
            "make_cost_total": cost_total,
            "retail_price_total": retail_price_total,
            "missing_sku": missing_sku,
            "data": full_output_list,
        }
        return totals_result

    def get_decontructed_sku_list(self, sku_array):
        sku_info_list = []
        for sku in sku_array:
            string_code = ""
            base_object = {}
            base_object["sku"] = sku
            each_sku_element = (sku.strip()).split(" ")
            string_code += f"{each_sku_element[0][0:2]}-{each_sku_element[0][2:]}"
            for element in each_sku_element[1:-1]:
                string_code += " " + element
            base_object["code"] = string_code

            base_object["size"] = each_sku_element[-1]
            sku_info_list.append(base_object)

        return sku_info_list

    def get_sku_prices_from_postgres(self, order_number):
        postgres = res.connectors.load("postgres")

        query = f"SELECT * FROM sell.order_line_item_pricing WHERE order_number = '{order_number}'"
        logger.info(f"running {query}")
        df = postgres.run_query(query)

        df["order_date"] = df["order_date"].apply(lambda x: x.isoformat())
        df = df.drop(["id", "order_id"], axis=1)

        df = df.rename(
            columns={"retail_price": "price", "revenue_share_amount": "revenue_share"}
        )

        for column, dtype in df.dtypes.items():
            if dtype == "object":
                try:
                    df[column] = df[column].astype(float)
                except ValueError:
                    continue

        lookup = {row["sku"]: row.to_dict() for _, row in df.iterrows()}

        logger.info(f"found skus: {lookup.keys()}")
        return lookup


def compare_lists_of_dicts(new, old):
    # Get all keys from both lists, assuming all dicts have the same keys
    k1 = [
        "make_cost",
        "price",
        "shipping",
        "order_type",
        "revenue_share",
        "total_amount",
        "sku",
    ]
    k2 = [
        "make_cost",
        "price",
        "shipping",
        "order_type",
        "revenue_share",
        "total_amount",
        "sku",
    ]
    match = True
    for i in range(0, len(k2)):
        vals_list1 = [d[k1[i]] for d in new]
        vals_list2 = [d[k2[i]] for d in old]

        #    Now, compare the two sets
        if vals_list1 == vals_list2:
            print(f"{k1[i]} match")
        else:
            match = False
            print(f"{k1[i]} MISMATCH!!")

    # If no mismatches are found, all keys and their values match

    return match


# if __name__ == "__main__":
#     record_ids = [
#         "recwfFOkHTc1xoNof",
#         "recm59mN5OXV6SfgW",
#         "recsf9bJNIF5I3K6u",
#         "recqnYLTFeTe4Kru8",
#         "rec4G4Bqv6vjwu1eD",
#         "recH9vrQRGBso1kk5",
#         "recRzIEEySxc78XYI",
#         "recsOAx661QsM3ULS",
#         "receXHFsfqkACPJEz",
#         "recaVysATcORj4xYD",
#         "recZm2hlRGu3K1EzV",
#         "reckBiCPXrhGDJsLV",
#         "recyikbgGPcg3Ylxk",
#         "recDfbr5q2O2udVhG",
#     ]

#     for record in record_ids:
#         event = {"order_id": record}
#         p = PaymentInvoiceCalculator()
#         retNew = p.get_prices_for_skus_on_order(event)
#         p = PaymentInvoiceCalculatorOld()
#         retOld = p.process(event)

#         compare_lists_of_dicts(
#             retNew["dict_array_details"], retOld["dict_array_details"]
#         )
