import re
from datetime import datetime

# from schemas.pydantic.brand import Brand
import pandas as pd

from schemas.pydantic.payments import BrandCreateUpdate

import res.connectors
import res.utils
from res.connectors.postgres.PostgresDictSync import PostgresDictSync
from res.utils import logger

FULFILLMENT_BASE_ID = "appfaTObyfrmPHvHc"
RES_META_BASE_ID = "appc7VJXjoGsSOjmw"

import pandas as pd


def update_existing_brands_from_airtable_to_postgres():
    logger.info("beginging update_existing_brands_from_airtable_to_postgres...")
    postgres = res.connectors.load("postgres")
    airtable = res.connectors.load("airtable")

    RES_META_BRAND_TABLE = airtable[RES_META_BASE_ID]["tblMnwRUuEvot969I"]

    #

    # dt_fulfillbase = FULFILLMENT_BRAND_TABLE.to_dataframe(filters=None)
    dt_metabase = RES_META_BRAND_TABLE.to_dataframe(filters=None)

    dt_metabase = dt_metabase.astype(object)

    dt_metabase = dt_metabase.where(pd.notnull(dt_metabase), None)
    logger.info("loaded from tblMnwRUuEvot969I...")
    list_of_dicts_metabase = dt_metabase.to_dict("records")

    # Iterate through each dictionary in the first list

    dicts_metabase_cols_to_keep = [
        "Name",
        "record_id",
        "_fulfillment_id",
        "Created At",
        "Code",
        "Shopify Store Name",
        "homepage_url",
        "Sell Enabled",
        "shopify_location_id_nyc",
        "Contact Email",
        "Created At Year",
        "Payments: Revenue Share (Ecom)",
        "Payments: Revenue Share (Wholesale)",
        "Subdomain Name",
        "Brand's create.ONE URL",
        "Shopify Shared Secret",
        "quickbooks_manufacturing_id",
        "Address",
        "is_direct_payment_default",
        "Brand Success Active",
        "Shopify API Key",
        "Shopify API Password",
        "register_brand_email_address",
    ]

    filtered_metabase = [
        {k: v for k, v in d.items() if k in dicts_metabase_cols_to_keep}
        for d in list_of_dicts_metabase
    ]

    for d in filtered_metabase:
        for k in d.keys():
            if isinstance(d[k], str):
                d[k] = replace_accents(d[k])

    rename_map = {
        "_fulfillment_id": "fulfill_record_id",
        "record_id": "meta_record_id",
        "created_at": "created_at_airtable",
        "code": "brand_code",
        "shopify_apikey": "shopify_api_key",
        "shopify_apipassword": "shopify_api_password",
    }

    for d in filtered_metabase:
        for k in list(d.keys()):
            new_key = re.sub(r"[\s.]+", "_", k).lower()
            new_key = re.sub(r"[\s:()']+", "", new_key).lower()
            d[new_key] = d.pop(k)

        for old_key, new_key in rename_map.items():
            if old_key in d:
                d[new_key] = d.pop(old_key)
        d["start_date"] = res.utils.dates.coerce_to_full_datetime(
            d["created_at_airtable"]
        ).date()
        d["end_date"] = None

    brands = []
    for d in filtered_metabase:
        d["created_at_airtable"] = res.utils.dates.coerce_to_full_datetime(
            d["created_at_airtable"]
        )
        # if d.get("created_at_airtable") >= res.utils.dates.date_parse("2024-03-20"):
        #     d["must_pay_before_make"] = True

        if not d.get("brand_code"):

            pass
        else:
            brands.append((BrandCreateUpdate(**d)))

    brand_dicts = [model.dict() for model in brands]

    for d in brand_dicts:
        d["airtable_brand_id"] = d["fulfill_record_id"]
        d["airtable_brand_code"] = d["brand_code"]

    brand_dicts = list({d["brand_code"]: d for d in brand_dicts}.values())

    logger.info(f"loaded {len(brand_dicts)} brands from airtable to sync")
    try:
        sync = PostgresDictSync(
            "sell.brands",
            brand_dicts,
            on="brand_code",
            pg=postgres,
            ignore_columns_for_update=[
                "id",
                "airtable_brand_code",
                "airtable_brand_id",
                "id_uuid",
                "modified_by",
                "active_subscription_id",
                "created_at",
                "is_brand_whitelist_payment",
                "updated_at",
                "api_key",
                "must_pay_before_make",
            ],
        )
        logger.info(f"Attempting sync...")

        sync.sync()
        logger.info(f"sync complete...")

        dfNewBrands = postgres.run_query(
            "Select * from sell.brands where created_at > '2024-03-20' "
        )

        logger.info(
            f"Select * from sell.brands where created_at > '2024-03-20'. Checking if need to override must_pay_before_make on new brands. dfNewBrands shape: {len(dfNewBrands.shape)}..."
        )
        for d in brand_dicts:
            if (
                d.get("created_at_airtable")
                >= res.utils.dates.date_parse("2024-03-20").date()
            ):
                db_pay_before_make = dfNewBrands[
                    dfNewBrands["brand_code"] == d["brand_code"]
                ].iloc[0]["must_pay_before_make"]
                logger.info(
                    f"for {d['brand_code']} db_pay_before_make: {db_pay_before_make}"
                )
                if db_pay_before_make is None:
                    logger.info(
                        f"update sell.brands SET must_pay_before_make = 'True'  where brand_code =  '{d['brand_code']}' "
                    )
                    postgres.run_update(
                        f"update sell.brands SET must_pay_before_make = 'True'  where brand_code =  '{d['brand_code']}'"
                    )
        logger.info(f"Brand Sync complete...")

    except:
        import traceback

        res.utils.logger.info(traceback.format_exc())
        slack_id_to_notify = " <@U04HYBREM28> "
        payments_api_slack_channel = "payments_api_notification"
        res.utils.ping_slack(
            f"[update_existing_brands_from_airtable_to_postgres error] {traceback.format_exc()} {slack_id_to_notify} ",
            payments_api_slack_channel,
        )
        raise


def replace_accents(text):
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "Á": "A",
        "É": "E",
        "Í": "I",
        "Ó": "O",
        "Ú": "U",
        "ñ": "n",
        "Ñ": "N",
        "ä": "a",
        "ë": "e",
        "ï": "i",
        "ö": "o",
        "ü": "u",
        "Ä": "A",
        "Ë": "E",
        "Ï": "I",
        "Ö": "O",
        "Ü": "U",
        "ç": "c",
        "Ç": "C",
        "ß": "ss",
        # Add more replacements here if needed
    }
    for accented_char, unaccented_char in replacements.items():
        text = text.replace(accented_char, unaccented_char)
    return text


if __name__ == "__main__":
    # import_brands_from_airtable_to_postgres()
    update_existing_brands_from_airtable_to_postgres()
