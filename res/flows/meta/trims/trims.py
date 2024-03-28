from typing import Optional
import res

from schemas.pydantic.payments import BrandAndSubscription

# from collections import defaultdict
import traceback
from res.utils import ping_slack
from res.utils.logging import logger
from . import queries

from tenacity import retry, wait_fixed, stop_after_attempt

from typing import Optional

from schemas.pydantic.trims import (
    Trim,
    TrimsInput,
    ReturnGetTrims,
    TrimImage,
    SqlSort,
    CreateTrimTaxonomy,
    ReturnTrimTaxonomySet,
    ReturnTrimTaxonomyList,
    TrimStatusQuery,
    TrimTypeQuery,
    SortFieldQuery,
    TrimSuppliedTypeQuery,
    BillOfMaterials,
)

from res.airtable.trims import (
    TRIMS,
    TRIM_TAXONOMY,
    TRIM_COLOR_MANAGEMENT,
    TRIM_BRAND,
    TRIM_VENDOR,
    BIN_LOCATION,
    TRIM_SIZE,
    BILL_OF_MATERIALS,
)


def get_trims(
    limit: int = 1,
    page_number: int = 1,
    id: Optional[str] = None,
    brand_code: Optional[str] = None,
    color_shape: Optional[str] = None,
    airtable_trim_taxonomy_id: Optional[str] = None,
    trim_name: Optional[str] = None,
    in_stock: Optional[bool] = None,
    sort: Optional[SqlSort] = None,
):
    """
    Get all trims.
    """
    import time

    start_time = time.time()  # Record the start time...

    postgres = res.connectors.load("postgres")

    query = queries.GET_ALL_POSTGRES_TRIMS

    offset = limit * (page_number - 1)

    query_addition = """
    WHERE t.name IS NOT NULL AND 
    t.status IS NOT NULL
    """

    params = []

    if id is not None:
        query_addition += """ 
        AND t.id = %s
        """
        params.append(id)

    if brand_code is not None:
        query_addition += """
        AND b.brand_code = %s
        """
        params.append(brand_code)

    if trim_name is not None:
        query_addition += """
          AND t.name LIKE %s
        """
        params.append("%" + trim_name + "%")

    if in_stock is not None:
        query_addition += """
        AND (t.warehouse_quantity + t.trim_node_quantity) > 0
        """

    if color_shape:
        get_airtable_colors = TRIM_COLOR_MANAGEMENT.all(
            formula=f"AND({{Shade Category}}='{color_shape}')"
        )

        if get_airtable_colors:
            list_of_color_ids = [color.get("id") for color in get_airtable_colors]
            query_addition += """
                AND t.airtable_color_id in %s
                """
            params.append("(" + ",".join(list_of_color_ids) + ")")

    if airtable_trim_taxonomy_id:
        airtable_taxonomy_ids = get_trim_taxonomy_ids(
            airtable_trim_taxonomy_id, postgres
        )
        if len(airtable_taxonomy_ids) > 0:
            query_addition += """
            AND t.airtable_trim_taxonomy_id in %s
            """
            params.append("(" + ",".join(airtable_taxonomy_ids) + ")")

    query += query_addition

    query += f""" 
        LIMIT {limit} 
        OFFSET {offset}
        """

    logger.info(f"query: {query}")

    get_postgres_trims_records = postgres.run_query(query, params)

    if sort:
        _sort_by = SortFieldQuery[sort.name].value
        is_sort_ascending = True if sort.direction == "ASC" else False
        get_postgres_trims_records = get_postgres_trims_records.sort_values(
            _sort_by, ascending=is_sort_ascending
        )

    list_of_records = []
    for record in get_postgres_trims_records.to_dict("records"):
        _warehouse_qty = record.get("warehouse_quantity", 0)
        _trim_node_qty = record.get("trim_node_quantity", 0)
        _available_qty = _warehouse_qty + _trim_node_qty

        trims = {
            "id": record.get("id"),
            "name": record.get("name"),
            "status": record.get("status"),
            "type": record.get("type"),
            "supplied_type": record.get("supplied_type"),
            "order_quantity": record.get("order_quantity"),
            "available_quantity": int(_available_qty),
            "warehouse_quantity": _warehouse_qty,
            "trim_node_quantity": _trim_node_qty,
            "in_stock": int(_available_qty) > 0,
        }

        airtable_trim_record = None
        airtable_trim_record = TRIMS.get(record.get("hash"))

        trim_record = airtable_trim_record.get("fields", None)

        if trim_record:
            trims["airtable_trim_base_id"] = trim_record.get("__record_id")
            trims["expected_delivery_date"] = trim_record.get("Expected Delivery Date")
            extract_trims = extract_trim_data(
                trim_record, record.get("sell_brands_pkid")
            )
            logger.info(f"extract_trims: {extract_trims}")
            trims.update(extract_trims)
            logger.info(f"trims: {trims}")
            list_of_records.append(ReturnGetTrims(**trims))

    end_time = time.time()  # Record the end time
    elapsed_time = end_time - start_time  # Calculate the elapsed time

    logger.info(f"get_trims took {elapsed_time} seconds to run")

    return list_of_records


def extract_trim_data(trim_record, sell_brands_pkid=None):
    trims = {}

    if trim_record.get("Image"):
        image = []
        for img in trim_record.get("Image"):
            image.append(TrimImage(url=img.get("url")))
        trims["image"] = image

    if trim_record.get("3D Image"):
        image = []
        for img in trim_record.get("3D Image"):
            image.append(TrimImage(url=img.get("url")))
        trims["image"] = image

    if trim_record.get("Brand"):
        trims["brand"] = {
            "id": sell_brands_pkid,
            "airtable_trim_base_id": trim_record.get("Brand", [""])[0],
            "name": trim_record.get("Brand Name", [""])[0],
            "code": trim_record.get("Brand Code", [""])[0],
        }
    if trim_record.get("Color"):
        trims["color"] = {
            "airtable_trim_base_id": trim_record.get("Color", [""])[0],
            "name": trim_record.get("Color Name", [""])[0],
            "hex_code": trim_record.get("Color Hex Code", [""])[0],
        }
    if trim_record.get("Size"):
        trims["size"] = {
            "airtable_trim_base_id": trim_record.get("Size", [""])[0],
            "name": trim_record.get("Size Name", [""])[0],
        }
    if trim_record.get("Trim Taxonomy"):
        trims["trim_taxonomy"] = {
            "id": trim_record.get("Trim Taxonomy", [""])[0],
            "airtable_trim_base_id": trim_record.get("Trim Taxonomy", [""])[0],
            "name": trim_record.get("Trim Taxonomy Key", [""])[0],
            "parent_id": trim_record.get("Trim Taxonomy Parent Id", [""])[0],
        }
    if trim_record.get("Vendor"):
        trims["vendor"] = {
            "id": trim_record.get("Vendor", [""])[0],
            "airtable_trim_base_id": trim_record.get("Vendor", [""])[0],
            "name": trim_record.get("Vendor Name", [""])[0],
        }

    return trims


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def create_trim(create_trim_input: TrimsInput):
    """
    Create a trim
    """
    import time

    logger.info(f"create_trim_input: {create_trim_input}")

    start_time = time.time()  # Record the start time...

    try:
        if (
            create_trim_input.brand_code is None
            and create_trim_input.airtable_brand_id is None
        ):
            raise Exception("Brand code or airtable_brand_id is required")

        postgres = res.connectors.load("postgres")

        trim_taxonomy = get_trim_taxonomy(create_trim_input.airtable_trim_taxonomy_id)
        trim_category = get_trim_taxonomy(create_trim_input.airtable_trim_category_id)

        trim_color = None
        if create_trim_input.airtable_color_id:
            trim_color = get_trim_color(create_trim_input.airtable_color_id)

        trim_vendor = get_trim_vendor(create_trim_input.airtable_vendor_id)
        trim_size = get_size(create_trim_input.airtable_size_id)
        brand = get_trims_base_brand(
            create_trim_input.brand_code, create_trim_input.airtable_brand_id
        )

        trim_payload = build_trim_payload(
            create_trim_input,
            brand,
            trim_color,
            trim_taxonomy,
            trim_vendor,
            trim_size,
            trim_category,
        )

        logger.info(f"trim_payload: {trim_payload}")

        # check if trim already exists
        trim = TRIMS.all(
            formula=f"AND({{Item Name}}='{trim_payload.get('Item Name')}')"
        )
        if trim:
            logger.info(f"trim: {trim}")
            raise Exception(f"Trim {trim_payload.get('Item Name')} already exists")

        brand_code = brand.get("fields").get("Code", None)

        if create_trim_input.brand_code:
            brand_code = create_trim_input.brand_code

        sql_trims_payload = sql_trim_payload(trim_payload, brand_code, postgres)
        logger.info(f"sql_trims_payload: {sql_trims_payload}")

        # get the id that will be used to create the record in postgres
        trim_payload["__postgres_id"] = res.utils.uuid_str_from_dict(sql_trims_payload)

        postgres.run_update(queries.INSERT_POSTGRES_TRIM, sql_trims_payload)

        logger.info(f"postgres id: {trim_payload['__postgres_id']}")

        create_record = {}
        create_record = TRIMS.create(trim_payload)
        logger.info(f"create_record: {create_record}")

        new_airtable_trim = create_record.get("fields")

        _status_from_enum = get_key_from_value(
            new_airtable_trim.get("Status", ""), TrimStatusQuery
        )

        _type_from_enum = get_key_from_value(
            new_airtable_trim.get("Type"), TrimTypeQuery
        )

        _supplied_type_from_enum = get_key_from_value(
            new_airtable_trim.get("Supplied Type", ""), TrimSuppliedTypeQuery
        )

        created_trim: Trim = {
            "id": trim_payload["__postgres_id"],
            "airtable_id": create_record.get("id", ""),
            "name": new_airtable_trim.get("Item Name", ""),
            "sell_brands_pkid": sql_trims_payload.get("sell_brands_pkid"),
            "status": _status_from_enum,
            "type": _type_from_enum,
            "supplied_type": _supplied_type_from_enum,
            "airtable_trim_base_meta_brand_id": new_airtable_trim.get("Brand", [None])[
                0
            ],
            "airtable_color_id": new_airtable_trim.get("Color", [""])[0],
            "airtable_trim_taxonomy_id": new_airtable_trim.get("Trim Taxonomy", [""])[
                0
            ],
            "airtable_size_id": new_airtable_trim.get("Size", [""])[0],
            "airtable_vendor_id": new_airtable_trim.get("Vendor", [""])[0],
            "image": new_airtable_trim.get("Image", []),
            "custom_brand_ai": new_airtable_trim.get("Custom Branding (AI File)", []),
            "order_quantity": new_airtable_trim.get("Order Quantity", 0),
            "expected_delivery_date": new_airtable_trim.get(
                "Expected Delivery Date", ""
            ),
        }

        ping_slack(
            f"[TRIMS-API] 🆕 Trims add 🏷️ {create_record.get('fields').get('Item Name')} Brand: {create_record.get('fields').get('Brand Code')}",
            "metabots",
        )
        logger.info(created_trim)
        end_time = time.time()  # Record the end time
        elapsed_time = end_time - start_time  # Calculate the elapsed time
        logger.info(f"create_trim took {elapsed_time} seconds to run")

        return Trim(**created_trim)

    except Exception as e:
        ping_slack(
            f"[TRIMS-API] ‼️ <@U018UETQEH1> Error creating trim 🏷️ ```{traceback.format_exc()}```",
            "metabots",
        )
        raise e


def update_trim(id: str, update_trim_input: Trim):
    """
    Update a trim
    """
    logger.info(f"update_trim_input: {update_trim_input}")
    try:
        trim_taxonomy = get_trim_taxonomy(update_trim_input.trim_taxonomy_id)
        trim_category = get_trim_taxonomy(update_trim_input.trim_category_id)
        trim_color = get_trim_color(update_trim_input.color_id)
        trim_vendor = get_trim_vendor(update_trim_input.vendor_id)
        brand = get_trims_base_brand(
            update_trim_input.brand_code, update_trim_input.airtable_brand_id
        )

        trim_payload = build_trim_payload(
            update_trim_input,
            brand,
            trim_color,
            trim_taxonomy,
            trim_vendor,
            trim_category,
        )

        logger.info(f"trim_payload: {trim_payload}")
        updated_record = TRIMS.update(id, trim_payload)

        # update postgres
        brand_code = brand.get("Code", None)
        if not brand_code:
            trim_record = TRIMS.get(id)
            brand_code = trim_record.get("fields").get("Brand Code", None)

        postgres = res.connectors.load("postgres")

        sql_trims_payload = sql_trim_payload(trim_payload, brand_code, postgres)
        sql_trims_payload["id"] = res.utils.uuid_str_from_dict(sql_trims_payload)
        logger.info(f"sql_trims_payload: {sql_trims_payload}")

        postgres.run_update(queries.UPDATE_POSTGRES_TRIM, sql_trims_payload)

        ping_slack(
            f"[TRIMS-API] 🚨 Trims updated 🏷️ {update_trim_input}",
            "metabots",
        )

        _status_from_enum = get_key_from_value(
            updated_record.get("Status", ""), TrimStatusQuery
        )

        _type_from_enum = get_key_from_value(updated_record.get("Type"), TrimTypeQuery)

        _supplied_type_from_enum = get_key_from_value(
            updated_record.get("Supplied Type", ""), TrimSuppliedTypeQuery
        )

        # build dict for return
        updated_trim: Trim = {
            "id": id,
            "airtable_id": updated_record.get("id", ""),
            "name": updated_record.get("Item Name"),
            "sell_brands_pkid": sql_trims_payload.get("sell_brands_pkid"),
            "status": _status_from_enum,
            "type": _type_from_enum,
            "supplied_type": _supplied_type_from_enum,
            "airtable_trim_base_meta_brand_id": updated_record.get("Brand", [None])[0],
            "airtable_color_id": updated_record.get("Color", [""])[0],
            "airtable_trim_taxonomy_id": updated_record.get("Trim Taxonomy", [""])[0],
            "airtable_size_id": updated_record.get("Size", [""])[0],
            "airtable_vendor_id": updated_record.get("Vendor", [""])[0],
            "image": updated_record.get("Image", []),
            "order_quantity": updated_record.get("Order Quantity", 0),
            "expected_delivery_date": updated_record.get("Expected Delivery Date", ""),
        }

        return Trim(**updated_trim)
    except Exception as e:
        raise e


def assign_bin_location(id: str):
    try:
        logger.info(f"Working with id: {id}")
        record = TRIMS.get(id)
        record = record["fields"]

        # get trim friendly name or Item name if Friendly name is blank
        trim_name = record.get("Item Name")
        if record.get("Trim Taxonomy Friendly Name"):
            trim_name = record.get("Trim Taxonomy Friendly Name")
        logger.info(f"Trim Type: {trim_name}")

        # get trims type
        trim_type = get_trim_type(trim_name)

        # get available location by type
        get_available_location = BIN_LOCATION.all(
            fomrula=f"AND({{Type}}='{trim_type}', {{Trims}}=BLANK())"
        )

        # if we have available location assign it to the trim
        if len(get_available_location) > 0:
            sorted_locations = sort_locations(get_available_location)
            location_id = sorted_locations[0].get("_record_id")
            location = sorted_locations[0].get("Key")
            logger.info(f"ID: {location_id} Location: {location}")
            updated_record = TRIMS.update(id, {"Bin Location": [location_id]})
            ping_slack(
                f"[TRIMS-API] 🚨 BIN Location Add 🏷️ {location} to {record.get('Item Name')}",
                "metabots",
            )
            return updated_record
        return None
    except Exception as e:
        raise e


def sort_locations(locations):
    return sorted(locations, key=lambda loc: (loc["Row"], loc["Number"]))


def get_trim_type(trim_type):
    types = ["Label", "Button", "Zipper", "Elastic", "Thread"]
    trim_type_lower = trim_type.lower()
    for type in types:
        if type.lower() in trim_type_lower:
            return type + "s" if type in ["Label", "Button", "Zipper"] else type
    return "Others"


def checkin_approval_trim(id: str):
    try:
        logger.info(f"Working with id: {id}")

        record = TRIMS.get(id)

        if record.get("Check-in Inventory"):
            updated_record = TRIMS.update(id, {"Status": "Approved"})
            ping_slack(
                f"[TRIMS-API] 🚨 Check-in Approval 🏷️ {record.get('Item Name')}",
                "metabots",
            )
            return updated_record
        return None
    except Exception as e:
        raise e


def build_trim_payload(
    trim_input, brand, trim_color, trim_taxonomy, trim_vendor, trim_size, trim_category
):
    s3 = res.connectors.load("s3")

    trim_payload = {}

    attributes_from_sub_tables = {
        "Brand": brand,
        "Color": trim_color,
        "Trim Taxonomy": trim_taxonomy,
        "Trim Category": trim_category,
        "Vendor": trim_vendor,
        "Size": trim_size,
    }

    for key, attr in attributes_from_sub_tables.items():
        if attr:
            record_id = attr.get("id")
            if record_id:
                trim_payload[key] = [record_id]

    attributes = {
        "Item Name": "name",
        "Status": "status",
        "Type": "type",
        "Image": "image",
        "Custom Branding (AI File)": "custom_brand_ai",
        "Supplied Type": "supplied_type",
        "Order Quantity": "order_quantity",
        "Expected Delivery Date": "expected_delivery_date",
        "Brand Color Name": "color_name_by_brand",
        "Brand Color Hex Code": "color_hex_code_by_brand",
        "Brand Vendor": "vendor_name",
    }

    for key, attr in attributes.items():
        value = getattr(trim_input, attr, None)

        if key == "Item Name":
            _trim_size_name = trim_size.get("fields").get("Name") if trim_size else ""
            _trim_brand_name = brand.get("fields").get("Name") if brand else ""
            _trim_category_name = (
                trim_category.get("fields").get("KEY") if trim_category else ""
            )
            _trim_color_name = (
                trim_input.color_name_by_brand if trim_input.color_name_by_brand else ""
            )

            _temp_name = [
                q
                for q in [
                    _trim_brand_name.strip(),
                    _trim_category_name.strip(),
                    _trim_size_name.strip(),
                    _trim_color_name.strip(),
                ]
                if q
            ]

            trim_payload[key] = " ".join(_temp_name)

        if value:
            trim_payload[key] = [value] if attr == "__record_id" else value

            if key == "Status":
                trim_payload[key] = TrimStatusQuery[value].value

            if key == "Type":
                trim_payload[key] = TrimTypeQuery[value].value

    # add material images
    if trim_input.image:
        trim_payload["Image"] = []
        for image in trim_input.image:
            trim_payload["Image"].append({"url": s3.generate_presigned_url(image.url)})

    if trim_input.custom_brand_ai:
        trim_payload["Custom Branding (AI File)"] = []
        for image in trim_input.custom_brand_ai:
            trim_payload["Custom Branding (AI File)"].append(
                {"url": s3.generate_presigned_url(image.url)}
            )

    trim_payload["Supplied Type"] = (
        "Brand Supplied" if trim_payload["Type"] == "Supplied" else "Resonance Supplied"
    )

    return trim_payload


def sql_trim_payload(trim_payload, brand_code, postgres=None):
    postgres = postgres or res.connectors.load("postgres")

    brand_record = None

    if brand_code:
        _brand_code = str(brand_code).upper()
        brand_record = _get_brand_from_postgres(_brand_code)

    sql_trims_payload = {
        "name": trim_payload.get("Item Name"),
        "sell_brands_pkid": brand_record.brand_pkid if brand_record else None,
        "status": trim_payload.get("Status"),
        "type": trim_payload.get("Type"),
        "supplied_type": trim_payload.get("Supplied Type"),
        "airtable_color_id": trim_payload.get("Color", [""])[0],
        "image_url": str(trim_payload.get("Image", [])),
        "order_quantity": int(trim_payload.get("Order Quantity") or 0),
        "trim_node_quantity": int(trim_payload.get("Trim Node Quantity") or 0),
        "warehouse_quantity": int(trim_payload.get("Warehouse Quantity") or 0),
        "airtable_size_id": trim_payload.get("Size", [""])[0],
        "airtable_trim_taxonomy_id": trim_payload.get("Trim Taxonomy", [""])[0],
        "color_name_by_brand": trim_payload.get("Brand Color Name", None),
        "color_hex_code_by_brand": trim_payload.get("Brand Color Hex Code", None),
        "expected_delivery_date": trim_payload.get("Expected Delivery Date", None),
        "vendor_name": trim_payload.get("Brand Vendor", None),
        "hash": f"{trim_payload.get('__record_id')}",
    }
    return sql_trims_payload


def get_key_from_value(value, pydantic_enum):
    for status in pydantic_enum:
        if status.value == value:
            return status.name
    return None


def _get_brand_from_postgres(brand_code):
    if brand_code is None:
        return None

    brand_data = res.utils.get_with_token(
        f"https://data.resmagic.io/payments-api/GetBrandDetails/?brand_code={brand_code}",
        "RES_PAYMENTS_API_KEY",
    )

    return BrandAndSubscription(**brand_data.json())


def get_trim_taxonomy(airtable_id=None):
    """
    Get trim taxonomy
    """

    if airtable_id is None:
        return None

    formula_filter = (
        f"AND(OR({{__record_id}}='{airtable_id}', {{__meta_ONE_id}}='{airtable_id}'))"
    )

    trim_taxonomy = TRIM_TAXONOMY.all(
        fields=["__record_id", "KEY", "__taxonomy_parent_id"], formula=formula_filter
    )

    if not trim_taxonomy:
        return None

    return trim_taxonomy[0]


def get_trim_color(airtable_id):
    """
    Get trim color
    """
    if airtable_id is None:
        return None

    formula_filter = f"AND({{__record_id}}='{airtable_id}')"

    trim_color = TRIM_COLOR_MANAGEMENT.all(
        fields=["__record_id", "Color Name", "Hex Code"], formula=formula_filter
    )

    if not trim_color:
        return None

    return trim_color[0]


def get_trim_vendor(airtable_id=None):
    """
    Get trim vendor
    """
    if airtable_id is None:
        return None

    formula_filter = f"AND(OR({{__record_id}}='{airtable_id}', {{__purchasing_record_id}}='{airtable_id}'))"

    trim_vendor = TRIM_VENDOR.all(fields=["__record_id", "KEY"], formula=formula_filter)

    if not trim_vendor:
        return None

    return trim_vendor[0]


def get_trims_base_brand(brand_code, airtable_id=None):
    """
    Get brand
    """
    if brand_code is None and airtable_id is None:
        return None

    formula_filter = f"AND({{Code}}='{brand_code}')"
    if airtable_id:
        formula_filter = (
            f"AND(OR({{__record_id}}='{airtable_id}',{{record_id}}='{airtable_id}'))"
        )

    brand = TRIM_BRAND.all(
        fields=["__record_id", "Name", "Code"],
        formula=formula_filter,
    )

    if not brand:
        return None

    return brand[0]


def get_size(airtable_id):
    """
    Get size
    """
    if airtable_id is None:
        return None

    formula_filter = f"AND({{__record_id}}='{airtable_id}')"

    size = TRIM_SIZE.all(
        fields=[
            "__record_id",
            "Name",
        ],
        formula=formula_filter,
    )

    if not size:
        return None

    return size[0]


def migrate_trim_taxonony_to_postgres():
    """
    Migrate Trim Taxonomy to Postgres
    """
    try:
        # Get all trim taxonomies from Airtable
        trim_taxonomies = TRIM_TAXONOMY.all(view="viwIjfLMT1HmtopaQ")

        # Initialize an empty list to store the taxonomy records
        taxonomy_records = []

        # Iterate through each trim taxonomy and build a record
        for taxonomy in trim_taxonomies:
            taxonomy_record = {
                "name": taxonomy.get("fields").get("KEY"),
                "type": taxonomy.get("fields").get("Trim"),
                "friendly_name": taxonomy.get("fields").get("Style BOM Friendly Name"),
                "airtable_trim_size": taxonomy.get("fields").get("Trim Size"),
                "airtable_record_id": taxonomy.get("fields").get("__record_id"),
            }

            # Append the record to the list
            taxonomy_records.append(taxonomy_record)

        # # Insert the records into Postgres
        postgres = res.connectors.load("postgres")

        for record in taxonomy_records:
            inserted_record = postgres.run_update(queries.INSERT_TRIM_TAXONOMY, record)
            TRIM_TAXONOMY.update(
                record.get("airtable_record_id"),
                {"__postgres_record_uuid": inserted_record.get("id")},
            )

        return "Trim Taxonomy records migrated to Postgres"

    except Exception as e:
        raise e


def update_trim_taxonony_to_postgres():
    """
    Update Trim Taxonomy to Postgres
    """
    try:
        # get all trims from postgres
        postgres = res.connectors.load("postgres")

        get_taxonomy_queries = queries.GET_TRIM_TAXONOMY_POSTGRES

        _get_taxonomy_queries = get_taxonomy_queries
        _get_taxonomy_queries += "WHERE parent_meta_trim_taxonomy_pkid = '0'"

        trims = postgres.run_query(_get_taxonomy_queries)
        logger.info(f"trims: {trims}")

        for trim in trims.to_dict("records"):
            # get airtable record from trim.get("airtable_record_id")
            logger.info(f"Trim: {trim}")
            airtable_record = TRIM_TAXONOMY.get(trim.get("airtable_record_id"))
            if airtable_record:
                airtable_record = airtable_record.get("fields")
                logger.info(
                    f"AND({{__meta_ONE_id}}='{airtable_record.get('__taxonomy_parent_id')}')"
                )
                get_parent_record = TRIM_TAXONOMY.all(
                    formula=f"AND({{__meta_ONE_id}}='{airtable_record.get('__taxonomy_parent_id')}')"
                )

                if get_parent_record:
                    logger.info(f"get_parent_record: {get_parent_record}")
                    parent_record = get_parent_record[0]
                    parent_record = parent_record.get("fields")
                    get_taxonomy_queries_by_id = (
                        get_taxonomy_queries
                        + f" WHERE airtable_record_id = '{parent_record.get('__record_id')}'  "
                    )
                    get_parent_taxonomy = postgres.run_query(get_taxonomy_queries_by_id)
                    logger.info(
                        f"get_parent_taxonomy: {get_parent_taxonomy.to_dict('records')}"
                    )
                    parent_id = "0"
                    if len(get_parent_taxonomy.to_dict("records")) > 0:
                        parent_taxonomy = get_parent_taxonomy.to_dict("records")[0]
                        logger.info(f"parent_taxonomy: {parent_taxonomy} ")
                        parent_id = parent_taxonomy.get("id")

                    update_payload = {
                        "id": trim.get("id"),
                        "name": airtable_record.get("KEY"),
                        "type": airtable_record.get("Trim"),
                        "friendly_name": airtable_record.get("Style BOM Friendly Name"),
                        "airtable_trim_size": airtable_record.get("Trim Size"),
                        "airtable_record_id": airtable_record.get("__record_id"),
                        "parent_meta_trim_taxonomy_pkid": parent_id,
                    }

                    logger.info(f"update_payload: {update_payload}")
                    # update postgres record
                    postgres.run_update(queries.UPDATE_TRIM_TAXONOMY, update_payload)

        # UPDATE_TRIM_TAXONOMY

        return "Trim Taxonomy records migrated to Postgres"

    except Exception as e:
        raise e


def create_trim_taxonomy(create_trim_taxonomy_input: CreateTrimTaxonomy):
    """
    Create a trim taxonomy
    """
    try:
        postgres = res.connectors.load("postgres")

        trim_taxonomy = {
            "name": create_trim_taxonomy_input.name,
            "type": create_trim_taxonomy_input.type,
            "friendly_name": create_trim_taxonomy_input.friendly_name,
            "parent_meta_trim_taxonomy_pkid": create_trim_taxonomy_input.parent_id,
            "airtable_record_id": create_trim_taxonomy_input.record_id,
            "airtable_trim_size": create_trim_taxonomy_input.trim_size,
        }

        trim_taxonomy["id"] = res.utils.uuid_str_from_dict(trim_taxonomy)

        postgres.run_update(queries.INSERT_TRIM_TAXONOMY, trim_taxonomy)

        return ReturnTrimTaxonomySet(**trim_taxonomy)

    except Exception as e:
        raise e


def update_trim_taxonomy(id: str, update_trim_taxonomy_input: CreateTrimTaxonomy):
    """
    Update a trim taxonomy
    """
    try:
        postgres = res.connectors.load("postgres")

        trim_taxonomy = {
            "name": update_trim_taxonomy_input.name,
            "type": update_trim_taxonomy_input.type,
            "friendly_name": update_trim_taxonomy_input.friendly_name,
            "parent_meta_trim_taxonomy_pkid": update_trim_taxonomy_input.parent_id,
            "airtable_record_id": update_trim_taxonomy_input.record_id,
            "airtable_trim_size": update_trim_taxonomy_input.trim_size,
            "id": id,
        }

        postgres.run_update(queries.UPDATE_TRIM_TAXONOMY, trim_taxonomy)

        return ReturnTrimTaxonomySet(**trim_taxonomy)

    except Exception as e:
        raise e


def get_trim_taxonomy_ids(airtable_trim_taxonomy_id, postgres):
    """
    Get trim taxonomy ids
    """
    params = []
    taxonomy_query = queries.GET_TRIM_TAXONOMY_POSTGRES

    taxonomy_query += " WHERE airtable_record_id = %s  "
    params.append(airtable_trim_taxonomy_id)

    get_postgres_trims_records = postgres.run_query(taxonomy_query, params)

    if len(get_postgres_trims_records.to_dict("records")) == 0:
        raise Exception(
            f"No trims found for the given taxonomy airtable_trim_taxonomy_id: {airtable_trim_taxonomy_id}"
        )

    record = get_postgres_trims_records.to_dict("records")[0]

    taxonomy = {
        "id": record.get("id"),
        "airtable_trim_base_id": record.get("airtable_record_id"),
        "name": record.get("name"),
        "trims": [],
    }
    logger.info(f"taxonomy: {taxonomy}")

    list_of_taxonomies = get_taxonomy_child(taxonomy, [], postgres)
    list_of_taxonomies.append(taxonomy)

    taxonomy_ids = [
        taxonomy.get("airtable_trim_base_id") for taxonomy in list_of_taxonomies
    ]

    return taxonomy_ids


def get_trims_by_trim_taxonomy(
    airtable_trim_taxonomy_id: Optional[str] = None,
):
    """
    Get trims by trim taxonomy
    """

    params = []
    taxonomy_query = queries.GET_TRIM_TAXONOMY_POSTGRES

    taxonomy_query += " WHERE airtable_record_id = %s  "
    params.append(airtable_trim_taxonomy_id)

    postgres = res.connectors.load("postgres")

    get_postgres_trims_records = postgres.run_query(taxonomy_query, params)

    if len(get_postgres_trims_records.to_dict("records")) == 0:
        raise Exception(
            f"No trims found for the given taxonomy airtable_trim_taxonomy_id: {airtable_trim_taxonomy_id}"
        )

    record = get_postgres_trims_records.to_dict("records")[0]

    taxonomy = {
        "id": record.get("id"),
        "airtable_trim_base_id": record.get("airtable_record_id"),
        "name": record.get("name"),
        "trims": [],
    }
    logger.info(f"taxonomy: {taxonomy}")

    list_of_taxonomies = get_taxonomy_child(taxonomy, [], postgres)
    list_of_taxonomies.append(taxonomy)

    logger.info(f"list_of_taxonomies: {list_of_taxonomies}")

    taxonomy_ids = [
        taxonomy.get("airtable_trim_base_id") for taxonomy in list_of_taxonomies
    ]

    formula_addtions = f"FIND({{__trim_taxonomy_id}}, '{','.join(taxonomy_ids)}')"

    logger.info(f"formula: {formula_addtions}")

    trims = TRIMS.all(formula=f"AND({formula_addtions})")

    list_of_records = build_taxonomy_payload(trims)

    return list_of_records


def get_list_of_taxonomies(airtable_trim_taxonomy_id: str):
    """
    Get trims by trim taxonomy
    """

    params = []
    taxonomy_query = queries.GET_TRIM_TAXONOMY_POSTGRES

    taxonomy_query += " WHERE airtable_record_id = %s  "
    params.append(airtable_trim_taxonomy_id)

    postgres = res.connectors.load("postgres")

    get_postgres_trims_records = postgres.run_query(taxonomy_query, params)

    if len(get_postgres_trims_records.to_dict("records")) == 0:
        raise Exception(
            f"No trims found for the given taxonomy airtable_trim_taxonomy_id: {airtable_trim_taxonomy_id}"
        )

    record = get_postgres_trims_records.to_dict("records")[0]

    taxonomy = {
        "id": record.get("id"),
        "airtable_trim_base_id": record.get("airtable_record_id"),
        "name": record.get("name"),
        "trims": [],
        "taxonomy_childs": [],
    }
    logger.info(f"taxonomy: {taxonomy}")

    list_of_taxonomies = get_taxonomy_child(taxonomy, [], postgres)
    taxonomy["taxonomy_childs"] = list_of_taxonomies

    get_airtable_trims_taxonomy = TRIMS.all(
        formula=f"AND({{__trim_taxonomy_id}}='{airtable_trim_taxonomy_id}')"
    )

    taxonomy["trims"] = build_taxonomy_payload(get_airtable_trims_taxonomy)

    # get trims for each child
    for child in taxonomy["taxonomy_childs"]:
        get_airtable_trims_taxonomy = TRIMS.all(
            formula=f"AND({{__trim_taxonomy_id}}='{child.get('airtable_trim_base_id')}')"
        )
        child["trims"] = build_taxonomy_payload(get_airtable_trims_taxonomy)

    logger.info(f"taxonomy: {taxonomy}")

    return ReturnTrimTaxonomyList(**taxonomy)


def build_taxonomy_payload(trims):
    list_of_records = []

    for record in trims:
        _avaialble_qty = 0
        _warehouse_qty = record.get("fields").get("Warehouse Quantity", 0)
        _trim_node_qty = record.get("fields").get("In Trim Node Quantity", 0)

        _avaialble_qty = _warehouse_qty + _trim_node_qty

        trims = {
            "id": record.get("fields").get("__postgres_id", ""),
            "airtable_id": record.get("id"),
            "name": record.get("fields").get("Item Name"),
            "status": record.get("fields").get("Status"),
            "type": record.get("fields").get("Type"),
            "supplied_type": record.get("fields").get("Supplied Type"),
            "order_quantity": record.get("fields").get("Order Quantity"),
            "expected_delivery_date": record.get("fields").get(
                "Expected Delivery Date"
            ),
            "available_quantity": int(_avaialble_qty),
            "warehouse_quantity": _warehouse_qty,
            "trim_node_quantity": _trim_node_qty,
            "in_stock": int(_avaialble_qty) > 0,
        }

        extract_trims = extract_trim_data(record.get("fields"))
        trims.update(extract_trims)
        list_of_records.append(ReturnGetTrims(**trims))
    return list_of_records


def get_taxonomy_child(taxonomy: dict, leafs: list, postgres):
    childs = get_child(taxonomy.get("id"), postgres)

    if not childs:
        leafs.append(taxonomy)
        return leafs

    for child in childs:
        taxonomy_childs = get_child(child.get("id"), postgres)
        if taxonomy_childs:
            for taxonomy_child in taxonomy_childs:
                child_taxonomy = {
                    "id": taxonomy_child.get("id"),
                    "airtable_trim_base_id": taxonomy_child.get("airtable_record_id"),
                    "name": taxonomy_child.get("name"),
                    "trims": [],
                }
                get_taxonomy_child(child_taxonomy, leafs, postgres)
        else:
            child_taxonomy = {
                "id": child.get("id"),
                "airtable_trim_base_id": child.get("airtable_record_id"),
                "name": child.get("name"),
                "trims": [],
            }
            leafs.append(child_taxonomy)
    return leafs


def get_child(id, postgres):
    query = queries.GET_TRIM_TAXONOMY_POSTGRES
    query += f"WHERE parent_meta_trim_taxonomy_pkid = '{id}'"

    trims = postgres.run_query(query)

    return trims.to_dict("records") if len(trims.to_dict("records")) > 0 else None


def get_trims_by_body_code(body_code: str):
    """
    Get trims by body code
    """
    try:

        get_bom_records = BILL_OF_MATERIALS.all(
            formula=f"AND({{body_code}}='{body_code}', {{trim_taxonomy_id}}!='')"
        )

        logger.info(f"len: {len(get_bom_records)}")

        if len(get_bom_records) == 0:
            raise Exception(f"No trims found for the given body code: {body_code}")

        list_of_records = []

        for record in get_bom_records:
            logger.info(f"ID: {record.get('id')}")
            trims = {
                "id": record.get("id"),
                "name": record.get("fields").get("Style BOM Friendly Name", [""])[0],
                "trim_type": record.get("fields").get("taxonomy trim type", [""])[0],
                "trim_taxonomy_id": record.get("fields").get("Trim Taxonomy 2.0", [""])[
                    0
                ],
                "trim_taxonomy_name": record.get("fields").get(
                    "Trim Taxonomy Name", [""]
                )[0],
                "quantity": record.get("fields").get("Trim Quantity") or 0,
                "length": record.get("fields").get("Trim Length") or 0,
            }

            logger.info(f"trims: {trims}")

            list_of_records.append(BillOfMaterials(**trims))

        return list_of_records

    except Exception as e:
        raise e
